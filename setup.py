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
    REQUIRES = ['configobj', 'six', 'pecan', 'setuptools >= 1.1.6',
                'cassandra-driver', 'pymongo', 'msgpack-python',
                'python-swiftclient', 'asyncio', 'aiohttp']
    setup(
        name='deuce',
        version='0.1',
        description='Deuce - Block-level de-duplication as-a-service',
        license='Apache License 2.0',
        url='github.com/rackerlabs/deuce',
        author='Rackspace',
        author_email='',
        include_package_data=True,
        install_requires=REQUIRES,
        test_suite='deuce',
        zip_safe=False,
        data_files=[('bin', ['config.py'])],
        packages=find_packages(exclude=['tests*', 'deuce/tests*'])
    )
