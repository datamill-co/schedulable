import inspect
from datetime import datetime

import sqlalchemy as sa
from croniter import croniter

NEXT_JOBS_SQL_TEMPLATE = '''
WITH next_jobs as (
    SELECT id
    FROM {table}
    WHERE
        {job_types}
        (scheduler_locked_at IS NULL OR
            ({now} > (scheduler_locked_at + INTERVAL '60 seconds'))) AND
        run_at <= {now} AND
        attempts < max_attempts AND
        (status = 'queued' OR
          (status = 'running' AND ({now} > (worker_locked_at + INTERVAL '1 second' * timeout))) OR
          (status = 'retry' AND ({now} > (worker_locked_at + INTERVAL '1 second' * retry_delay))))
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
    scheduler_locked_at = {now}
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
        worker_locked_at IS NULL
    LIMIT 1
    FOR UPDATE
)
UPDATE {table} SET
    status = 'running',
    worker_id = :worker_id,
    worker_locked_at = {now},
    started_at = COALESCE(worker_job.started_at, {now}),
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
        id = :id
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
UPDATE {table} SET
    worker_locked_at = {now}
FROM worker_job
WHERE {table}.id = worker_job.id
RETURNING {table}.*;
'''

SCHEDULER_LOCK_JOBS_SQL_TEMPLATE = '''
WITH scheduler_jobs as (
    SELECT id
    FROM {table}
    WHERE
        {job_types}
        (scheduler_locked_at IS NULL OR
            ({now} > (scheduler_locked_at + INTERVAL '60 seconds'))) AND
        status IN ({statuses})
    FOR UPDATE SKIP LOCKED
)
UPDATE {table} SET
    scheduler_locked_at = {now}
FROM scheduler_jobs
WHERE {table}.id = scheduler_jobs.id
RETURNING {table}.*;
'''

SCHEDULER_LOCK_SQL_TEMPLATE = '''
WITH schedulables as (
    SELECT id
    FROM {table}
    WHERE
        schedule_enabled = true AND
        schedule IS NOT NULL AND
        (scheduler_locked_at IS NULL OR
            ({now} > (scheduler_locked_at + INTERVAL '60 seconds')))
    FOR UPDATE SKIP LOCKED
)
UPDATE {table} SET
    scheduler_locked_at = {now}
FROM schedulables
WHERE {table}.id = schedulables.id
RETURNING {table}.*;
'''

def run_update_sql(session,
                   cls_self,
                   sql_template,
                   now=None,
                   sql_params=None,
                   additional_params=None):
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

    instances = (
        session
        .query(cls)
        .from_statement(sa.text(sql_smt))
        .params(**sql_params)
        .all()
    )
    session.commit()

    return instances

class Schedulable:
    schedule_enabled = sa.Column(sa.Boolean, nullable=False)
    schedule = sa.Column(sa.String)
    job_type = sa.Column(sa.String)
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

    def get_croniter(self, base_time=None):
        if not base_time:
            base_time = datetime.utcnow()
        return croniter(self.schedule, base_time)

    def next_run(self, base_time=None):
        return self.get_croniter(base_time).get_next(datetime)

    def last_run(self, base_time=None):
        return self.get_croniter(base_time).get_prev(datetime)

    @classmethod
    def schedule_next_runs(cls,
                           session,
                           logger=None,
                           schedulable_instance_class=None,
                           schedulable_instance_fk=None,
                           now=None):
        if not schedulable_instance_class:
            schedulable_instance_class = getattr(cls, '__schedulable_instance_class__')
        if not schedulable_instance_fk:
            schedulable_instance_fk = getattr(cls, '__schedulable_instance_fk__')
        schedulable_instance_fk_col = getattr(schedulable_instance_class, schedulable_instance_fk)

        schedulables = run_update_sql(session, cls, SCHEDULER_LOCK_SQL_TEMPLATE, now)

        for schedulable in schedulables:
            most_recent_instance = (
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

            if not most_recent_instance:
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
                    'retry_delay': schedulable.retry_delay,
                    'job_type': schedulable.job_type
                }
                instance_fields[schedulable_instance_fk] = schedulable.id

                new_instance = schedulable_instance_class(**instance_fields)
                session.add(new_instance)
                session.flush()

        session.commit()

## TODO: unique index
## TODO: other indexes?
class SchedulableInstance:
    scheduled = sa.Column(sa.Boolean, nullable=False, default=False)
    job_type = sa.Column(sa.String)
    run_at = sa.Column(sa.DateTime, nullable=False, default=datetime.utcnow)
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
        session.refresh(self)

    def touch(self, session, now=None):
        sql_params = {
            'id': self.id
        }
        run_update_sql(session, self, JOB_TOUCH_SQL_TEMPLATE, now, sql_params)
        session.refresh(self)

    def complete(self, session, status):
        self.status = status
        self.ended_at = datetime.utcnow()
        session.commit()

    def succeed(self, session):
        self.complete(session, 'success')

    def fail(self, session):
        self.complete(session, 'failed')

    @classmethod
    def next_jobs(cls,
                  session,
                  job_types=None,
                  now=None,
                  max_jobs=1):
        additional_params = {}
        if job_types:
            additional_params['job_types'] = '"job_type" IN (\'{}\')'.format(
                "','".join(job_types))
        else:
            additional_params['job_types'] = ''

        sql_params = {
            'max_jobs': max_jobs
        }

        return run_update_sql(session,
                              cls,
                              NEXT_JOBS_SQL_TEMPLATE,
                              now,
                              sql_params,
                              additional_params)
