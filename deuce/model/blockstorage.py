import deuce
from deuce.model import Vault
from deuce.model.block import Block
from deuce.util import log as logging
import deuce.transport.wsgi.errors as errors

logger = logging.getLogger(__name__)


class BlockStorage(object):

    def __init__(self, vault_id):
        self.vault_id = vault_id
        self.Vault = Vault.get(vault_id)

    def get_metadata_id(self, storage_block_id):
        return deuce.metadata_driver.get_block_metadata_id(self.vault_id,
                                                           storage_block_id)

    def delete_block(self, storage_block_id):
        # TODO: Implement this, error from here is just temporary
        raise errors.HTTPNotImplemented(
            'Directly Delete Block From Storage Not Implemented')
        # return False

    def head_block(self, storage_block_id):
        # TODO: Implement this, error from here is just temporary
        raise errors.HTTPNotImplemented(
            'Directly Delete Block From Storage Not Implemented')
        # return {'x': 'y'}

    def get_block(self, storage_block_id):
        """Get a block directly from storage
        """
        metadata_id = self.get_metadata_id(storage_block_id)

        obj = deuce.storage_driver.get_block_obj(self.vault_id,
                                                 storage_block_id)

        return Block(self.vault_id, metadata_id, obj) if obj else None

    @staticmethod
    def get_blocks_generator(marker, limit):
        # TODO: Implement this, error from here is just temporary
        raise errors.HTTPNotImplemented(
            'Directly Delete Block From Storage Not Implemented')
        # return []
