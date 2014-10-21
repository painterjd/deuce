import os

import deuce
from deuce.util import log as logging


logger = logging.getLogger(__name__)


class Block(object):

    def __init__(self, vault_id, metadata_block_id, obj=None,
            storage_block_id=None):
        self.vault_id = vault_id
        self.metadata_block_id = metadata_block_id
        self.storage_block_id = storage_block_id
        self._fileobj = obj

    def get_obj(self):
        """Returns a file-like object that can be used for
        reading the data. The stream should be closed when
        by the caller when done.
        """
        return self._fileobj

    def get_ref_count(self):
        """Returns the number of references to this block
        """
        return deuce.metadata_driver.get_block_ref_count(
            self.vault_id, self.metadata_block_id)

    def get_ref_modified(self):
        """Returns the last modification time of this block
        """
        return deuce.metadata_driver.get_block_ref_modified(
            self.vault_id, self.metadata_block_id)

    def get_block_length(self):
        """Returns the length of this block from storage
        """
        storage_id = self.get_storage_id()
        return deuce.storage_driver.get_block_object_length(
            self.vault_id, storage_id)

    def get_storage_id(self):
        """Returns the storage id for a given block"""
        if self.metadata_block_id is not None:
            return deuce.metadata_driver.get_block_storage_id(
                self.vault_id, self.metadata_block_id)
        else:
            return self.storage_block_id
