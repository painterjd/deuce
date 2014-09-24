# -*- coding: utf-8 -*-
import sys
try:
    from distutils.core import setup
    from setuptools import find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages
else:
    REQUIRES = ['configobj', 'falcon', 'six', 'setuptools >= 1.1.6',
                'cassandra-driver', 'pymongo', 'msgpack-python',
                'python-swiftclient', 'python-logstash', 'asyncio',
                'aiohttp']
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
        data_files=[('config', ['./ini/config.ini', './ini/configspec.ini'])],
        include_package_data=True,
        packages=find_packages(exclude=['tests*', 'deuce/tests*']),
    )
