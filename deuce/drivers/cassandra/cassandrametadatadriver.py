
import importlib
import six
import uuid

from deuce.drivers.metadatadriver import MetadataStorageDriver
from deuce.drivers.metadatadriver import GapError, OverlapError
from pecan import conf

import deuce

CQL_CREATE_FILE = '''
    INSERT INTO files (projectid, vaultid, fileid, finalized, size)
    VALUES (%s, %s, %s, false, %s)
'''

CQL_GET_FILE = '''
    SELECT finalized
    FROM files
    WHERE projectid = %s
    AND vaultid = %s
    AND fileid = %s
'''

CQL_GET_FILE_SIZE = '''
    SELECT size
    FROM files
    WHERE projectid = %s
    AND vaultid = %s
    AND fileid = %s
'''

CQL_DELETE_FILE = '''
    DELETE FROM files
    where projectid = %s
    AND vaultid = %s
    AND fileid = %s
'''

CQL_GET_ALL_FILE_BLOCKS = '''
    SELECT blockid, offset
    FROM fileblocks
    WHERE projectid = %s
    AND vaultid = %s
    AND fileid = %s
    ORDER BY offset
'''

CQL_GET_COUNT_ALL_FILE_BLOCKS = '''
    SELECT COUNT(DISTINCT(blockid))
    FROM fileblocks
    WHERE projectid = %s
    AND vaultid = %s
'''

CQL_GET_FILE_BLOCKS = '''
    SELECT blockid, offset
    FROM fileblocks
    WHERE projectid = %s
    AND vaultid = %s
    AND fileid = %s
    AND offset >= %s
    ORDER BY offset
    LIMIT %s
'''

CQL_GET_ALL_FILE_BLOCKS_W_SIZE = '''
    SELECT blockid, offset, blocksize
    FROM fileblocks
    WHERE projectid = %s
    AND vaultid = %s
    AND fileid = %s
    ORDER by offset
'''

CQL_GET_ALL_BLOCKS = '''
    SELECT blockid
    FROM blocks
    WHERE projectid = %s
    AND vaultid = %s
    AND blockid >= %s
    order by blockid
    LIMIT %s
'''

CQL_GET_COUNT_ALL_BLOCKS = '''
    SELECT COUNT(DISTINCT(blockid))
    FROM blocks
    WHERE projectid = %s
    AND vaultid = %s
'''

CQL_GET_ALL_FILES_MARKER = '''
    SELECT fileid
    FROM files
    WHERE projectid=%s
    AND vaultid = %s
    AND fileid >= %s
    AND finalized = %s
    LIMIT %s
'''

CQL_GET_ALL_FILES = '''
    SELECT fileid
    FROM files
    WHERE projectid=%s
    AND vaultid = %s
    AND finalized = %s
    LIMIT %s
'''

CQL_GET_COUNT_ALL_FILES = '''
    SELECT COUNT(DISTINCT(fileid))
    FROM files
    WHERE projectid=%s
    AND vaultid = %s
'''

CQL_FINALIZE_FILE = '''
    UPDATE files
    SET finalized=true,
    size=%s
    WHERE projectid=%s
    AND vaultid=%s
    AND fileid=%s
'''

CQL_ASSIGN_BLOCK_TO_FILE = '''
    INSERT INTO fileblocks
    (projectid, vaultid, fileid, blockid, blocksize, offset)
    VALUES (%s, %s, %s, %s, %s, %s)
'''

CQL_REGISTER_BLOCK = '''
    INSERT INTO blocks
    (projectid, vaultid, blockid, blocksize)
    values (%s, %s, %s, %s)
'''

CQL_UNREGISTER_BLOCK = '''
    DELETE FROM blocks
    where projectid=%s
    AND vaultid=%s
    AND blockid=%s
'''

CQL_GET_BLOCK_SIZE = '''
    SELECT blocksize FROM blocks
    WHERE
    projectid = %s
    AND vaultid = %s
    AND blockid = %s
'''

# TODO: Optimize this. Now need to
# count all of the blocks, we can just
# get the first, limit by 1, and
# return on that.
CQL_HAS_BLOCK = '''
    SELECT count(*)
    FROM blocks
    WHERE projectid = %s
    AND vaultid = %s
    AND blockid = %s
'''


class CassandraStorageDriver(MetadataStorageDriver):

    def __init__(self):
        # Import the driver module.
        self.cassandra = importlib.import_module(
            conf.metadata_driver.cassandra.db_module)

        # Import the cluster submodule
        cluster_module = importlib.import_module(
            '{0}.cluster'.format(conf.metadata_driver.cassandra.db_module))

        self._cluster = cluster_module.Cluster(
            conf.metadata_driver.cassandra.cluster)

        deuce_keyspace = conf.metadata_driver.cassandra.keyspace
        self._session = self._cluster.connect(deuce_keyspace)

    def _determine_limit(self, limit):
        """ Determines the limit based on user input """

        # Note: +1 is allowed here because it allows
        # the user to fetch one beyond to see if they
        # are at the end of the list
        if not limit:
            res = conf.api_configuration.max_returned_num + 1
        else:
            res = min(conf.api_configuration.max_returned_num + 1, limit)

        return res

    def get_vault_statistics(self, vault_id):
        """Return the statistics on the vault.

        "param vault_id: The ID of the vault to gather statistics for"""
        res = {}

        args = {
            'projectid': deuce.context.project_id,
            'vaultid': vault_id
        }

        def __stats_query(cql_statement, default_value):
            result = self._session.execute(cql_statement, args)
            try:
                return result[0][0]

            except IndexError:  # pragma: no cover
                return default_value

        def __stats_get_vault_file_block_count():
            return __stats_query(CQL_GET_COUNT_ALL_FILE_BLOCKS, 0)

        def __stats_get_vault_file_count():
            return __stats_query(CQL_GET_COUNT_ALL_FILES, 0)

        def __stats_get_vault_block_count():
            return __stats_query(CQL_GET_COUNT_ALL_BLOCKS, 0)

        res['file-blocks'] = {}
        res['file-blocks']['count'] = __stats_get_vault_file_block_count()

        # Add any statistics regarding files
        res['files'] = {}
        res['files']['count'] = __stats_get_vault_file_count()

        # Add any statistics regarding blocks
        res['blocks'] = {}
        res['blocks']['count'] = __stats_get_vault_block_count()

        # Add any statistics specific to the Cassandra backend
        res['internal'] = {}

        return res

    def create_file(self, vault_id, file_id):
        """Creates a new file with no blocks and no files"""
        args = (deuce.context.project_id, vault_id, uuid.UUID(file_id), 0)
        res = self._session.execute(CQL_CREATE_FILE, args)

        return file_id

    def file_length(self, vault_id, file_id):
        """Retrieve the length of the file."""
        args = (deuce.context.project_id, vault_id, uuid.UUID(file_id))

        res = self._session.execute(CQL_GET_FILE_SIZE, args)

        try:
            return int(res[0][0])
        except IndexError:
            return 0

    def has_file(self, vault_id, file_id):
        args = (deuce.context.project_id, vault_id, uuid.UUID(file_id))

        res = self._session.execute(CQL_GET_FILE, args)

        return len(res) > 0

    def is_finalized(self, vault_id, file_id):
        args = (deuce.context.project_id, vault_id, uuid.UUID(file_id))
        res = self._session.execute(CQL_GET_FILE, args)

        try:
            row = res[0]
            return row[0] == 1
        except IndexError:
            return False

    def delete_file(self, vault_id, file_id):
        args = (deuce.context.project_id, vault_id, uuid.UUID(file_id))

        self._session.execute(CQL_DELETE_FILE, args)

    def finalize_file(self, vault_id, file_id, file_size=None):
        """Updates the files table to set a file to finalized. This function
        makes no assumptions about whether or not the file record actually
        exists"""

        # Check for gaps and overlaps.
        expected_offset = 0

        args = (deuce.context.project_id, vault_id, uuid.UUID(file_id))

        res = self._session.execute(CQL_GET_ALL_FILE_BLOCKS_W_SIZE, args)

        for blockid, offset, size in res:

            # Use one last chance to check for the block size
            # if it is not in the fileblocks row.
            if size is None:
                size = self._get_block_size(vault_id, blockid)

                # If size is None, the block was never registered so we
                # skip this record. This will likely result in a GapError
                # being thrown on the next pass
                if size is None:
                    continue

            if offset == expected_offset:
                expected_offset += size
            elif offset < expected_offset:  # Block overlaps previous block
                raise OverlapError(deuce.context.project_id, vault_id,
                    file_id, blockid, startpos=offset, endpos=expected_offset)
            else:  # There is a gap between this block and the previous one
                raise GapError(deuce.context.project_id, vault_id, file_id,
                    startpos=expected_offset, endpos=offset)

        # Now we must check the very last block and ensure
        # that is completes the file. This is only doable if
        # the final file size was provided
        if file_size and file_size != expected_offset:

            if expected_offset < file_size:  # Gap
                raise GapError(deuce.context.project_id, vault_id, file_id,
                    startpos=expected_offset, endpos=file_size)

            else:
                assert expected_offset > file_size

                # This means that the "last" block overlaps
                # the end of the file.
                raise OverlapError(deuce.context.project_id, vault_id, file_id,
                    blockid, startpos=file_size, endpos=expected_offset)

        if self.has_file(vault_id, file_id):
            if file_size is None:
                file_size = 0
            args = (file_size, deuce.context.project_id, vault_id,
                uuid.UUID(file_id))
            res = self._session.execute(CQL_FINALIZE_FILE, args)

    def get_block_data(self, vault_id, block_id):
        args = (deuce.context.project_id, vault_id, block_id)

        res = self._session.execute(CQL_GET_BLOCK_SIZE, args)

        try:
            return dict(blocksize=res[0][0])
        except IndexError:
            raise Exception("No such block: {0}".format(block_id))

    def _get_block_size(self, vault_id, block_id):
        """Returns the size of the specified block. If the block
        is not found, None is returned"""

        args = (deuce.context.project_id, vault_id, block_id)

        res = self._session.execute(CQL_GET_BLOCK_SIZE, args)

        try:
            return res[0][0]
        except IndexError:
            return None

    def get_file_data(self, vault_id, file_id):
        """Returns a tuple representing data for this file"""
        args = (deuce.context.project_id, vault_id, uuid.UUID(file_id))

        res = self._session.execute(CQL_GET_FILE, args)

        try:
            row = res[0]
        except IndexError:
            raise Exception("No such file: {0}".format(file_id))

        return row

    def has_block(self, vault_id, block_id):
        retval = False

        args = (deuce.context.project_id, vault_id, block_id)
        res = self._session.execute(CQL_HAS_BLOCK, args)
        cnt = res[0]
        return cnt[0] > 0

    def create_block_generator(self, vault_id, marker=None,
            limit=None):

        args = (deuce.context.project_id, vault_id, marker or '0',
                self._determine_limit(limit))

        res = self._session.execute(CQL_GET_ALL_BLOCKS, args)

        return [row[0] for row in res]

    def create_file_generator(self, vault_id, marker=None, limit=None,
            finalized=True):

        if marker is None:
            args = (deuce.context.project_id, vault_id, finalized,
                    self._determine_limit(limit))

            query = CQL_GET_ALL_FILES
        else:
            args = (deuce.context.project_id, vault_id, uuid.UUID(marker),
                    finalized, self._determine_limit(limit))

            query = CQL_GET_ALL_FILES_MARKER

        res = self._session.execute(query, args)

        return [str(row[0]) for row in res]

    def create_file_block_generator(self, vault_id, file_id,
                                    offset=None, limit=None):

        if limit is None:
            args = (deuce.context.project_id, vault_id, uuid.UUID(file_id))

            query = CQL_GET_ALL_FILE_BLOCKS
        else:
            args = (deuce.context.project_id, vault_id, uuid.UUID(file_id),
                    offset or 0, self._determine_limit(limit))

            query = CQL_GET_FILE_BLOCKS

        query_res = self._session.execute(query, args)

        return [(row[0], row[1]) for row in query_res]

    def assign_block(self, vault_id, file_id, block_id, offset):

        blocksize = self._get_block_size(vault_id, block_id)

        # Note: blocksize can be None if the block does not yet exist. This
        # will probably not be allowed in the future, but for now we allow
        # this to be compatible with the other drivers.

        args = (deuce.context.project_id, vault_id, uuid.UUID(file_id),
                block_id, blocksize, offset)

        self._session.execute(CQL_ASSIGN_BLOCK_TO_FILE, args)

    def register_block(self, vault_id, block_id, blocksize):
        if not self.has_block(vault_id, block_id):
            args = (deuce.context.project_id, vault_id, block_id, blocksize)
            res = self._session.execute(CQL_REGISTER_BLOCK, args)

    def unregister_block(self, vault_id, block_id):
        args = (deuce.context.project_id, vault_id, block_id)

        res = self._session.execute(CQL_UNREGISTER_BLOCK, args)
