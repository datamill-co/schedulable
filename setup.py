#!/usr/bin/env python

from distutils.core import setup
from setuptools import find_packages

setup(
    name='schedulable',
    version='0.0.1',
    packages=find_packages(),
    install_requires=[
        'croniter==0.3.29'
    ],
    extras_require={
    },
    tests_require=[
    ]
)
