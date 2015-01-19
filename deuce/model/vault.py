from deuce.model.block import Block
from deuce.model.file import File
from deuce.model.exceptions import ConsistencyError
from deuce.util import log as logging

import deuce
import uuid
import hashlib


logger = logging.getLogger(__name__)


class Vault(object):

    @staticmethod
    def get(vault_id):

        if deuce.storage_driver.vault_exists(vault_id):
            return Vault(vault_id)

        return None

    @staticmethod
    def get_vaults_generator(marker, limit):
        return deuce.metadata_driver.create_vaults_generator(
            marker, limit)

    @staticmethod
    def create(vault_id):
        """Creates the vault with the specified vault_id"""
        deuce.storage_driver.create_vault(vault_id)
        deuce.metadata_driver.create_vault(vault_id)
        return Vault(vault_id)

    def __init__(self, vault_id):
        self.id = vault_id

    def _get_storage_id(self, block_id):
        return deuce.metadata_driver.get_block_storage_id(self.id, block_id)

    def get_vault_statistics(self):
        # Get information about the vault
        # - number of files
        # - number of blocks
        # - total size
        # - etc
        vault_stats = {}

        metadata_info = deuce.metadata_driver
        storage_info = deuce.storage_driver

        vault_stats['metadata'] = metadata_info.get_vault_statistics(
            self.id)
        vault_stats['storage'] = storage_info.get_vault_statistics(
            self.id)

        return vault_stats

    def put_block(self, block_id, blockdata, data_len):

        # Validate the hash of the block data against block_id
        if hashlib.sha1(blockdata).hexdigest() != block_id:
            raise ValueError('Invalid Hash Value in the block ID')

        actual_block_length = len(blockdata)
        if actual_block_length != data_len:
            raise BufferError(
                'Specified block length ({0}) does not match '
                'actual block length ({1})'.format(
                    data_len, actual_block_length))

        retval, storage_id = deuce.storage_driver.store_block(
            self.id, block_id, blockdata)

        if retval:
            deuce.metadata_driver.register_block(
                self.id, block_id, storage_id, data_len)

        return (retval, storage_id)

    def put_async_block(self, block_ids, blockdatas):
        block_ids = [block_id.decode() for block_id in block_ids]
        block_sizes = [len(block_data) for block_data in blockdatas]

        # Validate the hash of the block data against block_id
        for block_id, blockdata in zip(block_ids, blockdatas):
            if hashlib.sha1(blockdata).hexdigest() != block_id:
                raise ValueError('Invalid Hash Value in the block ID')

        retval, storage_ids = deuce.storage_driver.store_async_block(
            self.id,
            block_ids,
            blockdatas)

        retblocks = []

        # (BenjamenMeyer): If we fail to upload any one block then we
        # let the Validation and Clean-Up Service remove any uploaded blocks
        # and fail out the request as a whole
        if retval:
            # Note: zip() will produce a list of the shortest combination.
            # That is, if a = (1, 2, 3) and b = (a, b) then
            # zip(a, b) = ((1, a), (2, b)). If 'a' is the storage id for '2'
            # because '1' failed to be stored, then the entire list is
            # improperly shifted and we incorrectly report which blocks were
            # saved, thus corrupting the data
            for block_id, storageid, blockdata, block_size in zip(block_ids,
                                                                  storage_ids,
                                                                  blockdatas,
                                                                  block_sizes):
                logger.info('Project {0}, Vault {1}: Associating metadata '
                            'block {2} with storage block {3}'
                            .format(deuce.context.project_id,
                                    self.id,
                                    block_id,
                                    storageid))
                deuce.metadata_driver.register_block(
                    self.id,
                    block_id,
                    storageid,
                    block_size)
                retblocks.append((block_id, storageid))

        return (retval, retblocks)

    def get_blocks(self, marker, limit):
        gen = deuce.metadata_driver.create_block_generator(
            self.id, marker=marker, limit=limit)

        return (Block(self.id, bid) for bid in gen)

    def has_block(self, block_id, check_storage=False):
        if self._meta_has_block(block_id):
            if check_storage:
                if not self._storage_has_block(block_id):

                    # Record in metadata that the block is bad
                    deuce.metadata_driver.mark_block_as_bad(
                        self.id, block_id)

                    raise ConsistencyError(deuce.context.project_id,
                                           self.id, block_id,
                                           msg='Block does not exist'
                                               ' in Block Storage')
            return True
        else:
            return False

    def _storage_has_block(self, block_id):
        return deuce.storage_driver.block_exists(self.id,
            self._get_storage_id(block_id))

    def _meta_has_block(self, block_id):
        return deuce.metadata_driver.has_block(self.id, block_id)

    def get_block(self, block_id):
        storage_id = self._get_storage_id(block_id)
        obj = deuce.storage_driver.get_block_obj(self.id, storage_id)

        return Block(self.id, block_id, obj) if obj else None

    def get_blocks_generator(self, block_ids):
        storage_ids = [
            self._get_storage_id(block_id) for block_id in block_ids]
        return deuce.storage_driver.create_blocks_generator(
            self.id, storage_ids)

    def delete_block(self, vault_id, block_id):
        storage_id = self._get_storage_id(block_id)
        deuce.metadata_driver.unregister_block(vault_id, block_id)

        succ_storage = deuce.storage_driver.delete_block(vault_id,
                                                         storage_id)
        return succ_storage

    def create_file(self):
        file_id = str(uuid.uuid4())
        file_id = deuce.metadata_driver.create_file(self.id, file_id)

        return File(self.id, file_id)

    def get_files(self, marker, limit):
        gen = deuce.metadata_driver.create_file_generator(self.id,
            marker=marker, limit=limit, finalized=True)

        return (File(self.id, bid, finalized=True)
                for bid in gen)

    def get_file(self, file_id):
        try:
            data = deuce.metadata_driver.get_file_data(self.id, file_id)

        except:
            # TODO: Improve this. This could be very
            # dangerous and cause a lot of head-scratching.
            return None

        return File(self.id, file_id, finalized=data[0])

    def get_file_length(self, file_id):
        return deuce.metadata_driver.file_length(self.id, file_id)

    def delete(self):
        succ = deuce.storage_driver.delete_vault(self.id)
        if succ:
            deuce.metadata_driver.delete_vault(self.id)
        return succ

    def delete_file(self, file_id):
        return deuce.metadata_driver.delete_file(self.id, file_id)
