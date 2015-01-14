from deuce.drivers.cassandra import CassandraStorageDriver
from deuce.tests.test_sqlite_storage_driver import SqliteStorageDriverTest
from deuce import conf

from mock import patch

import unittest

# Explanation:
#   - The SqliteStorageDriver is the reference metadata driver. All
# other drivers should conform to the same interface, therefore
# we simply extend the SqliteStorageTest and run the sqlite driver tests
# against the Cassandra driver. The sqlite tests simply exercise the
# interface.


class CassandraStorageDriverTest(SqliteStorageDriverTest):

    def create_driver(self):
        return CassandraStorageDriver()

    @unittest.skipIf(conf.metadata_driver.cassandra.testing.is_mocking is False
       and conf.metadata_driver.cassandra.ssl_enabled is False,
       "Don't run the test if we are running without SSL")
    def test_create_driver_auth_ssl(self):
        with patch.object(conf.metadata_driver.cassandra, 'ssl_enabled',
                          return_value=True):
            with patch.object(conf.metadata_driver.cassandra, 'auth_enabled',
                              return_value=True):
                return CassandraStorageDriver()
