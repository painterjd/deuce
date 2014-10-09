import deuce
from deuce.model import Vault
from deuce.model.block import Block
from deuce.util import log as logging
import deuce.transport.wsgi.errors as errors

logger = logging.getLogger(__name__)


class BlockStorage(object):

    @staticmethod
    def get(vault_id):
        vault = Vault.get(vault_id)

        return BlockStorage(vault) if vault else None

    def __init__(self, vault):
        self.Vault = vault

    @property
    def vault_id(self):
        return self.Vault.id

    def get_metadata_id(self, storage_block_id):
        return deuce.metadata_driver.get_block_metadata_id(self.vault_id,
                                                           storage_block_id)

    def delete_block(self, storage_block_id):
        # TODO: Implement this, error from here is just temporary
        raise errors.HTTPNotImplemented(
            'Directly Delete Block From Storage Not Implemented')
        # return False

    def head_block(self, storage_block_id):

        # this gets the block from storage so it's verified there
        block = self.get_block(storage_block_id)

        # Block doesn't exist in storage
        if block is None:
            logger.debug('Unable to locate block {0}'.format(
                storage_block_id))
            return None

        # Default values
        storage_block_info = {
            'reference': {
                'count': 0,
                'modified': None
            },
            'id': {
                'storage': storage_block_id,
                'metadata': None
            },
            'length': 0
        }

        # Block exists in some form...
        if block.metadata_block_id is not None:
            # Block Exists in Metadata and storage
            storage_block_info['reference']['count'] = \
                block.get_ref_count()
            storage_block_info['reference']['modified'] = \
                block.get_ref_modified()
            storage_block_info['id']['metadata'] = block.metadata_block_id
            storage_block_info['length'] = block.get_block_length()

        else:
            # Block exists in only in storage (orphaned)
            storage_block_info['length'] = \
                deuce.storage_driver.get_block_object_length(self.vault_id,
                                                             storage_block_id)

        return storage_block_info

    def get_block(self, storage_block_id):
        """Get a block directly from storage
        """
        metadata_block_id = self.get_metadata_id(storage_block_id)

        obj = deuce.storage_driver.get_block_obj(self.vault_id,
                                                 storage_block_id)

        return Block(self.vault_id, metadata_block_id, obj) if obj else None

    @staticmethod
    def get_blocks_generator(marker, limit):
        # TODO: Implement this, error from here is just temporary
        raise errors.HTTPNotImplemented(
            'Directly Delete Block From Storage Not Implemented')
        # return []
