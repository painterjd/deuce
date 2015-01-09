# -*- coding: utf-8 -*-
import sys

from setuptools import setup, find_packages

REQUIRES = ['configobj', 'falcon', 'six', 'setuptools >= 1.1.6',
            'cassandra-driver', 'pymongo', 'msgpack-python',
            'python-swiftclient', 'asyncio',
            'aiohttp', 'stoplight']
setup(
    name='deuce',
    version='0.2',
    description='Deuce - Block-level de-duplication as-a-service',
    license='Apache License 2.0',
    url='github.com/rackerlabs/deuce',
    author='Rackspace',
    author_email='',
    install_requires=REQUIRES,
    test_suite='deuce',
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'deuce-server = deuce.cmd.server:run',
        ]
    },
    data_files=[('config', ['ini/config.ini', 'ini/configspec.ini'])],
    packages=find_packages(exclude=['tests*', 'deuce/tests*'])
)
