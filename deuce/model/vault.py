
from pecan import conf, response

from deuce.model.block import Block
from deuce.model.file import File

import deuce
import uuid
import hashlib


class Vault(object):

    @staticmethod
    def get(vault_id, auth_token=None):

        if deuce.storage_driver.vault_exists(vault_id,
                auth_token=auth_token):
            return Vault(vault_id)

        return None

    @staticmethod
    def create(vault_id, auth_token=None):
        """Creates the vault with the specified vault_id"""
        deuce.storage_driver.create_vault(vault_id,
                auth_token=auth_token)
        return Vault(vault_id)

    def __init__(self, vault_id):
        self.id = vault_id

    def get_vault_statistics(self, auth_token):
        # Get information about the vault
        # - number of files
        # - number of blocks
        # - number of file-blocks
        # - total size
        # - etc
        vault_stats = {}

        metadata_info = deuce.metadata_driver
        storage_info = deuce.storage_driver

        vault_stats['metadata'] = metadata_info.get_vault_statistics(
            self.id)
        vault_stats['storage'] = storage_info.get_vault_statistics(
            self.id, auth_token)

        return vault_stats

    def put_block(self, block_id, blockdata, data_len,
            auth_token=None):

        # Validate the hash of the block data against block_id
        if hashlib.sha1(blockdata).hexdigest() != block_id:
            raise ValueError('Invalid Hash Value in the block ID')

        retval = deuce.storage_driver.store_block(
            self.id, block_id, blockdata,
            auth_token=auth_token)

        file_id = deuce.metadata_driver.register_block(
            self.id, block_id, data_len)

        return retval

    def get_blocks(self, marker, limit):
        gen = deuce.metadata_driver.create_block_generator(
            self.id, marker=marker, limit=limit)

        return (Block(self.id, bid) for bid in gen)

    def get_block(self, block_id, auth_token=None):
        obj = deuce.storage_driver.get_block_obj(self.id,
            block_id, auth_token=auth_token)

        return Block(self.id, block_id, obj) if obj else None

    def get_block_length(self, block_id, auth_token=None):
        return deuce.storage_driver.get_block_object_length(
            self.id, block_id, auth_token=auth_token)

    def get_blocks_generator(self, block_ids, auth_token=None):
        return deuce.storage_driver.create_blocks_generator(
            self.id, block_ids,
            auth_token=auth_token)

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

    def delete(self, auth_token=None):
        return deuce.storage_driver.delete_vault(self.id,
            auth_token=auth_token)

    def delete_file(self, file_id):
        return deuce.metadata_driver.delete_file(self.id, file_id)
