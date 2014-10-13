import deuce
from deuce.model import Block
from deuce.model import Vault
from deuce.model.block import Block
from deuce.util import log as logging
from deuce.drivers.metadatadriver import ConstraintError
import deuce.transport.wsgi.errors as errors

logger = logging.getLogger(__name__)


class BlockStorage(object):

    def __init__(self, vault_id, storage_block_id=None):
        self.vault_id = vault_id
        self.Vault = Vault.get(vault_id)
        self.storage_block_id = storage_block_id

    def get_metadata_id(self, storage_block_id):
        return deuce.metadata_driver.get_block_metadata_id(self.vault_id,
                                                           storage_block_id)

    def delete_block(self, storage_block_id):

        block_id = self.get_metadata_id(storage_block_id)
        block = Block(self.vault_id, block_id) if block_id else None
        ref_count = block.get_ref_count() if block else None

        if block is None:

            return deuce.storage_driver.delete_block(self.vault_id,
                                                     storage_block_id)
        else:

            msg = "Storage ID: {0} has {1} " \
                  "reference(s) in metadata".format(
                      storage_block_id,
                      ref_count)

            raise ConstraintError(deuce.context.project_id,
                                  self.vault_id,
                                  msg)

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

    def get_blocks_generator(self, marker, limit):
        return (BlockStorage(self.vault_id, storage_block_id)
                for storage_block_id in
                deuce.storage_driver.get_vault_block_list(self.vault_id,
                                                          limit,
                                                          marker))
