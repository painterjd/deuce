import deuce
from deuce.model import Block
from deuce.model import Vault
from deuce.util import log as logging
from deuce.drivers.metadatadriver import ConstraintError
import deuce.transport.wsgi.errors as errors

logger = logging.getLogger(__name__)


class BlockStorage(object):

    @staticmethod
    def get(vault_id):
        vault = Vault.get(vault_id)

        return BlockStorage(vault) if vault else None

    def __init__(self, vault, storage_block_id=None):
        self.Vault = vault
        self.storage_block_id = storage_block_id

    @property
    def vault_id(self):
        return self.Vault.id

    def get_metadata_id(self, storage_block_id):
        return deuce.metadata_driver.get_block_metadata_id(self.vault_id,
                                                           storage_block_id)

    def delete_block(self, storage_block_id):

        block_id = self.get_metadata_id(storage_block_id)
        block = Block(self.vault_id,
                      block_id,
                      storage_block_id=storage_block_id) if block_id else None
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
            'length': 0,
            'orphaned': True,
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
            storage_block_info['orphaned'] = False

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
        return Block(self.vault_id,
                     metadata_block_id,
                     obj=obj,
                     storage_block_id=storage_block_id) if obj else None

    def get_blocks_generator(self, marker, limit):
        return (BlockStorage(Vault.get(self.vault_id), storage_block_id)
                for storage_block_id in
                deuce.storage_driver.get_vault_block_list(self.vault_id,
                                                          limit,
                                                          marker))
