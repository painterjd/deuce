from deuce.drivers.cassandra import CassandraStorageDriver
from deuce.tests.test_sqlite_storage_driver import SqliteStorageDriverTest


# Explanation:
#   - The SqliteStorageDriver is the reference metadata driver. All
# other drivers should conform to the same interface, therefore
# we simply extend the SqliteStorageTest and run the sqlite driver tests
# against the Cassandra driver. The sqlite tests simply exercise the
# interface.
class CassandraStorageDriverTest(SqliteStorageDriverTest):

    def create_driver(self):
        return CassandraStorageDriver()
