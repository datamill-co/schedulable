import uuid
import inspect
from datetime import datetime

import sqlalchemy as sa
from sqlalchemy.orm import declared_attr
from sqlalchemy.future import select
from sqlalchemy.dialects.postgresql import UUID
from croniter import croniter

NEXT_JOBS_SQL_TEMPLATE = '''
WITH next_jobs as (
    SELECT id
    FROM {table}
    WHERE
        {job_types}
        (scheduler_locked_at IS NULL OR
            ((scheduler_locked_at + INTERVAL '60 seconds') < {now})) AND
        run_at <= {now} AND
        attempts < max_attempts AND
        (status = 'queued' OR
          (status = 'running' AND ((worker_locked_at + INTERVAL '1 second' * timeout) < {now})) OR
          (status = 'retry' AND ((worker_locked_at + INTERVAL '1 second' * retry_delay) < {now})))
    ORDER BY
        CASE WHEN priority = 'critical'
             THEN 1
             WHEN priority = 'high'
             THEN 2
             WHEN priority = 'normal'
             THEN 3
             WHEN priority = 'low'
             THEN 4
        END,
        run_at
    LIMIT :max_jobs
    FOR UPDATE SKIP LOCKED
)
UPDATE {table} SET
    scheduler_locked_at = {now},
    scheduler_lock_id = '{lock_id}'
FROM next_jobs
WHERE {table}.id = next_jobs.id
RETURNING {table}.*;
'''

TIMEOUT_JOBS = '''
WITH next_jobs as (
    SELECT id
    FROM {table}
    WHERE
        {job_types}
        (scheduler_locked_at IS NULL OR
            ((scheduler_locked_at + INTERVAL '60 seconds') < {now})) AND
        attempts >= max_attempts AND
        status = 'running' AND
        (worker_locked_at + INTERVAL '1 second' * timeout) < {now}
    LIMIT :max_jobs
    FOR UPDATE SKIP LOCKED
)
UPDATE {table} SET
    status = 'failed',
    worker_id = NULL,
    worker_locked_at = NULL,
    ended_at = {now}
FROM next_jobs
WHERE {table}.id = next_jobs.id
RETURNING {table}.*;
'''

JOB_LOCK_SQL_TEMPLATE = '''
with worker_job as (
    SELECT id, started_at
    FROM {table}
    WHERE
        id = :id AND
        (
            scheduler_locked_at IS NULL OR
                ((scheduler_locked_at + INTERVAL '60 seconds') < {now})
        ) AND
        (
            (worker_id IS NULL AND
                worker_locked_at IS NULL) OR
            worker_id = :worker_id OR
            (worker_locked_at + INTERVAL '1 second' * timeout) < now()
        )
    LIMIT 1
    FOR UPDATE
)
UPDATE {table} SET
    status = 'running',
    worker_id = :worker_id,
    worker_locked_at = {now},
    scheduler_lock_id = null,
    scheduler_locked_at = null,
    started_at = COALESCE(worker_job.started_at, {now}),
    ended_at = NULL,
    attempts = attempts + 1
FROM worker_job
WHERE {table}.id = worker_job.id
RETURNING {table}.*;
'''

JOB_TOUCH_SQL_TEMPLATE = '''
with worker_job as (
    SELECT id
    FROM {table}
    WHERE
        id = :id AND
        worker_id = :worker_id
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
UPDATE {table} SET
    worker_locked_at = {now}
FROM worker_job
WHERE {table}.id = worker_job.id
RETURNING {table}.*;
'''

SCHEDULER_LOCK_SQL_TEMPLATE = '''
WITH schedulables as (
    SELECT id
    FROM {table}
    WHERE
        {enabled_column} = true AND
        schedule IS NOT NULL AND
        (scheduler_locked_at IS NULL OR
            ((scheduler_locked_at + INTERVAL '60 seconds') < {now}))
    FOR UPDATE SKIP LOCKED
)
UPDATE {table} SET
    scheduler_locked_at = {now},
    scheduler_lock_id = '{lock_id}'
FROM schedulables
WHERE {table}.id = schedulables.id
RETURNING {table}.*;
'''

SCHEDULER_UNLOCK = '''
UPDATE {table} SET
    scheduler_locked_at = null,
    scheduler_lock_id = null
WHERE scheduler_lock_id = '{lock_id}'
RETURNING {table}.*;
'''

def run_update_sql_prepare(
    cls_self,
    sql_template,
    now,
    sql_params,
    additional_params):
    if not now:
        now_sql = 'NOW()'
    else:
        now_sql = ':now'

    sql_smt = sql_template.format(
        table=cls_self.__tablename__,
        now=now_sql,
        **(additional_params or {}))

    if not sql_params:
        sql_params = {}

    if now:
        sql_params['now'] = now

    if inspect.isclass(cls_self):
        cls = cls_self
    else:
        cls = cls_self.__class__

    return cls, sql_smt, sql_params

def run_update_sql(
    session,
    cls_self,
    sql_template,
    now=None,
    sql_params=None,
    additional_params=None):
    cls, sql_smt, sql_params = run_update_sql_prepare(
        cls_self,
        sql_template,
        now,
        sql_params,
        additional_params)

    instances = (
        session
        .query(cls)
        .from_statement(sa.text(sql_smt))
        .params(**sql_params)
        .all()
    )
    session.commit()

    return instances

async def run_update_sql_async(
    session,
    cls_self,
    sql_template,
    now=None,
    sql_params=None,
    additional_params=None):
    cls, sql_smt, sql_params = run_update_sql_prepare(
        cls_self,
        sql_template,
        now,
        sql_params,
        additional_params)

    stmt = select(cls).from_statement(sa.text(sql_smt)).params(**sql_params)
    result = await session.execute(stmt)
    instances = []
    for row in result.all():
        instances.append(row[0])

    return instances

class SchedulerUnlockMixin:
    @classmethod
    def scheduler_unlock(cls,
                         session,
                         lock_id,
                         now=None):
        run_update_sql(
            session,
            cls,
            SCHEDULER_UNLOCK,
            now,
            additional_params={
                'lock_id': lock_id
            })

    @classmethod
    async def scheduler_unlock_async(cls,
                                     session,
                                     lock_id,
                                     now=None):
        await run_update_sql_async(
            session,
            cls,
            SCHEDULER_UNLOCK,
            now,
            additional_params={
                'lock_id': lock_id
            })

class Schedulable(SchedulerUnlockMixin):
    __schedulable_instance_class__ = None
    __schedulable_instance_fk__ = None
    __job_type_column__ = None # string column
    __enabled_column__ = None # boolean column

    schedule = sa.Column(sa.String)
    num_retries = sa.Column(sa.Integer, nullable=False, default=0)
    timeout = sa.Column(sa.Integer, nullable=False, default=3600)
    retry_delay = sa.Column(sa.Integer, nullable=False, default=600)
    priority = sa.Column(sa.Enum('critical',
                                 'high',
                                 'normal',
                                 'low',
                                 name='schedulable_priorities'),
                         nullable=False,
                         default='normal')
    scheduler_locked_at = sa.Column(sa.DateTime, index=True)
    scheduler_lock_id = sa.Column(UUID, index=True)

    def get_croniter(self, base_time=None):
        if not base_time:
            base_time = datetime.utcnow()
        return croniter(self.schedule, base_time)

    def next_run(self, base_time=None):
        return self.get_croniter(base_time).get_next(datetime)

    def last_run(self, base_time=None):
        return self.get_croniter(base_time).get_prev(datetime)

    @classmethod
    def schedule_next_runs_prepare(cls,
                                   schedulable_instance_class=None,
                                   schedulable_instance_fk=None,
                                   schedulable_instance_job_type=None):
        if not schedulable_instance_class:
            schedulable_instance_class = getattr(cls, '__schedulable_instance_class__')
        if not schedulable_instance_fk:
            schedulable_instance_fk = getattr(cls, '__schedulable_instance_fk__')
        schedulable_instance_fk_col = getattr(schedulable_instance_class, schedulable_instance_fk)
        if not schedulable_instance_job_type:
            schedulable_instance_job_type = getattr(schedulable_instance_class, '__job_type_column__')
        schedulable_instance_job_type_col = getattr(schedulable_instance_class, schedulable_instance_job_type)

        return (
            schedulable_instance_class,
            schedulable_instance_fk,
            schedulable_instance_fk_col,
            schedulable_instance_job_type,

        )

    @classmethod
    def get_schedulables(cls, session, lock_id, now):
        return run_update_sql(
            session,
            cls,
            SCHEDULER_LOCK_SQL_TEMPLATE,
            now,
            additional_params={
                'enabled_column': cls.__enabled_column__,
                'lock_id': lock_id
            })

    @classmethod
    async def get_schedulables_async(cls, session, lock_id, now):
        return await run_update_sql_async(
            session,
            cls,
            SCHEDULER_LOCK_SQL_TEMPLATE,
            now,
            additional_params={
                'enabled_column': cls.__enabled_column__,
                'lock_id': lock_id
            })

    @classmethod
    def get_most_recent_instance(cls,
                                 session,
                                 schedulable,
                                 schedulable_instance_class,
                                 schedulable_instance_fk_col):
        return (
            session
            .query(schedulable_instance_class)
            .filter(
                schedulable_instance_fk_col == schedulable.id,
                schedulable_instance_class.scheduled == True,
                schedulable_instance_class.status.notin_(['dequeued', 'success', 'failed'])
            )
            .order_by(schedulable_instance_class.run_at.desc())
            .first()
        )

    @classmethod
    async def get_most_recent_instance_async(cls,
                                             session,
                                             schedulable,
                                             schedulable_instance_class,
                                             schedulable_instance_fk_col):
        stmt = (
            select(schedulable_instance_class)
            .filter(
                schedulable_instance_fk_col == schedulable.id,
                schedulable_instance_class.scheduled == True,
                schedulable_instance_class.status.notin_(['dequeued', 'success', 'failed'])
            )
            .order_by(schedulable_instance_class.run_at.desc())
            .limit(1)
        )
        result = await session.execute(stmt)
        row = result.one_or_none()

        if not row:
            return None

        return row[0]

    @classmethod
    def make_instance_from_schedule(cls,
                                    logger,
                                    schedulable,
                                    schedulable_instance_fk,
                                    schedulable_instance_job_type,
                                    schedulable_instance_class,
                                    now):
        run_at = schedulable.next_run(now)

        if logger:
            logger.info('Scheduling next run for {} at {}'.format(
                cls.__name__,
                run_at.isoformat()))

        instance_fields = {
            'scheduled': True,
            'run_at': run_at,
            'unique': 'scheduled:{}:{}'.format(
                schedulable.id,
                int(run_at.timestamp())),
            'max_attempts': schedulable.num_retries + 1,
            'priority': schedulable.priority,
            'timeout': schedulable.timeout,
            'retry_delay': schedulable.retry_delay
        }
        instance_fields[schedulable_instance_fk] = schedulable.id
        job_type = getattr(schedulable, getattr(schedulable, '__job_type_column__'))
        instance_fields[schedulable_instance_job_type] = job_type

        if hasattr(cls, '__schedulable_extra_fields__'):
            for cls_field, instance_field in getattr(cls, '__schedulable_extra_fields__').items():
                instance_fields[instance_field] = getattr(schedulable, cls_field)

        return schedulable_instance_class(**instance_fields)

    @classmethod
    def schedule_next_runs(cls,
                           session,
                           logger=None,
                           schedulable_instance_class=None,
                           schedulable_instance_fk=None,
                           schedulable_instance_job_type=None,
                           now=None):
        schedulable_instance_class, schedulable_instance_fk,schedulable_instance_fk_col, schedulable_instance_job_type = cls.schedule_next_runs_prepare(
            schedulable_instance_class,
            schedulable_instance_fk,
            schedulable_instance_job_type)

        lock_id = str(uuid.uuid4())

        schedulables = cls.get_schedulables(session, now)

        session.commit()

        for schedulable in schedulables:
            try:
                most_recent_instance = cls.get_most_recent_instance(
                    session,
                    schedulable,
                    schedulable_instance_class,
                    schedulable_instance_fk_col)

                if not most_recent_instance:
                    new_instance = cls.make_instance_from_schedule(
                        logger,
                        schedulable,
                        schedulable_instance_fk,
                        schedulable_instance_job_type,
                        schedulable_instance_class,
                        now)
                    
                    session.add(new_instance)
                    session.commit()
            except:
                if logger:
                    logger.exception('Exception scheduling instance: {}'.format(schedulable.id))
                session.rollback()

        if schedulables:
            cls.scheduler_unlock(
                session,
                lock_id,
                now)
            session.commit()

    @classmethod
    async def schedule_next_runs_async(cls,
                                       session,
                                       logger=None,
                                       schedulable_instance_class=None,
                                       schedulable_instance_fk=None,
                                       schedulable_instance_job_type=None,
                                       now=None):
        schedulable_instance_class, schedulable_instance_fk,schedulable_instance_fk_col, schedulable_instance_job_type = cls.schedule_next_runs_prepare(
            schedulable_instance_class,
            schedulable_instance_fk,
            schedulable_instance_job_type)

        lock_id = str(uuid.uuid4())

        schedulables = await cls.get_schedulables_async(session, lock_id, now)

        await session.commit()

        jobs_scheduled = 0
        for schedulable in schedulables:
            try:
                most_recent_instance = await cls.get_most_recent_instance_async(
                    session,
                    schedulable,
                    schedulable_instance_class,
                    schedulable_instance_fk_col)

                if not most_recent_instance:
                    new_instance = cls.make_instance_from_schedule(
                        logger,
                        schedulable,
                        schedulable_instance_fk,
                        schedulable_instance_job_type,
                        schedulable_instance_class,
                        now)

                    session.add(new_instance)
                    await session.commit()

                    jobs_scheduled += 1
            except:
                if logger:
                    logger.exception('Exception scheduling instance: {}'.format(schedulable.id))
                await session.rollback()

        if schedulables:
            await cls.scheduler_unlock_async(
                session,
                lock_id,
                now)
            await session.commit()

        return jobs_scheduled

class SchedulableInstance(SchedulerUnlockMixin):
    __job_type_column__ = None # string column

    scheduled = sa.Column(sa.Boolean, nullable=False, default=False)
    run_at = sa.Column(sa.DateTime, nullable=False, index=True, default=datetime.utcnow)
    started_at = sa.Column(sa.DateTime)
    ended_at = sa.Column(sa.DateTime)
    status = sa.Column(sa.Enum('queued',
                               'pushed',
                               'running',
                               'retry',
                               'dequeued',
                               'failed',
                               'success',
                               name='schedulable_statuses'),
                       nullable=False,
                       default='queued')
    priority = sa.Column(sa.Enum('critical',
                                 'high',
                                 'normal',
                                 'low',
                                 name='schedulable_priorities'),
                         nullable=False,
                         default='normal')
    unique = sa.Column(sa.String)
    scheduler_locked_at = sa.Column(sa.DateTime, index=True)
    scheduler_lock_id = sa.Column(UUID, index=True)
    worker_locked_at = sa.Column(sa.DateTime, index=True)
    worker_id = sa.Column(sa.Text)
    attempts = sa.Column(sa.Integer, nullable=False, default=0)
    max_attempts = sa.Column(sa.Integer, nullable=False, default=1)
    timeout = sa.Column(sa.Integer, nullable=False)
    retry_delay = sa.Column(sa.Integer, nullable=False)

    def lock(self, session, worker_id, now=None):
        sql_params = {
            'id': self.id,
            'worker_id': worker_id
        }
        run_update_sql(session, self, JOB_LOCK_SQL_TEMPLATE, now, sql_params)
        session.commit()
        session.refresh(self)

    async def lock_async(self, session, worker_id, now=None):
        sql_params = {
            'id': self.id,
            'worker_id': worker_id
        }
        await run_update_sql_async(session, self, JOB_LOCK_SQL_TEMPLATE, now, sql_params)
        await session.commit()
        await session.refresh(self)

    def touch(self, session, worker_id, now=None):
        sql_params = {
            'id': self.id,
            'worker_id': worker_id
        }
        run_update_sql(session, self, JOB_TOUCH_SQL_TEMPLATE, now, sql_params)
        session.commit()
        session.refresh(self)

    async def touch_async(self, session, worker_id, now=None):
        sql_params = {
            'id': self.id,
            'worker_id': worker_id
        }
        await run_update_sql_async(session, self, JOB_TOUCH_SQL_TEMPLATE, now, sql_params)
        await session.commit()
        await session.refresh(self)

    def complete(self, status):
        self.status = status
        self.ended_at = datetime.utcnow()

    def succeed(self):
        self.complete('success')

    def fail(self):
        self.complete('failed')

    @classmethod
    def next_jobs_prepare(cls,
                          lock_id,
                          job_types,
                          max_jobs):
        additional_params = {
            'lock_id': lock_id
        }
        if job_types:
            additional_params['job_types'] = '"job_type" IN (\'{}\')'.format(
                "','".join(job_types))
        else:
            additional_params['job_types'] = ''

        sql_params = {
            'max_jobs': max_jobs
        }

        return sql_params, additional_params

    @classmethod
    def next_jobs(cls,
                  session,
                  lock_id,
                  job_types=None,
                  now=None,
                  max_jobs=1):
        sql_params, additional_params = cls.next_jobs_prepare(
            lock_id,
            job_types,
            max_jobs)

        return run_update_sql(session,
                              cls,
                              NEXT_JOBS_SQL_TEMPLATE,
                              now,
                              sql_params,
                              additional_params)

    @classmethod
    async def next_jobs_async(cls,
                              session,
                              lock_id,
                              job_types=None,
                              now=None,
                              max_jobs=1):
        sql_params, additional_params = cls.next_jobs_prepare(
            lock_id,
            job_types,
            max_jobs)

        return await run_update_sql_async(
            session,
            cls,
            NEXT_JOBS_SQL_TEMPLATE,
            now,
            sql_params,
            additional_params)

    @classmethod
    def timeout_jobs_prepare(cls,
                             job_types,
                             max_jobs):
        additional_params = {}
        if job_types:
            additional_params['job_types'] = '"job_type" IN (\'{}\')'.format(
                "','".join(job_types))
        else:
            additional_params['job_types'] = ''

        sql_params = {
            'max_jobs': max_jobs
        }

        return sql_params, additional_params

    @classmethod
    def timeout_jobs(cls,
                     session,
                     job_types=None,
                     now=None,
                     max_jobs=1):
        sql_params, additional_params = cls.timeout_jobs_prepare(
            job_types,
            max_jobs)

        return run_update_sql(session,
                              cls,
                              TIMEOUT_JOBS,
                              now,
                              sql_params,
                              additional_params)

    @classmethod
    async def timeout_jobs_async(cls,
                                 session,
                                 job_types=None,
                                 now=None,
                                 max_jobs=1):
        sql_params, additional_params = cls.timeout_jobs_prepare(
            job_types,
            max_jobs)

        return await run_update_sql_async(
            session,
            cls,
            TIMEOUT_JOBS,
            now,
            sql_params,
            additional_params)

    @declared_attr
    def __table_args__(cls):
        index_name = 'idx_{}_unique_job'.format(cls.__tablename__)
        extra_unique_columns = getattr(cls, '__extra_unique_columns__', [])
        columns = extra_unique_columns + [cls.unique]

        return (
            sa.Index(index_name,
                     *columns,
                     unique=True,
                     postgresql_where=cls.status.in_(
                        ['queued', 'pushed', 'running', 'retry'])),
        )
