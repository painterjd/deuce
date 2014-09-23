
import deuce

import os


class Block(object):

    def __init__(self, vault_id, block_id, obj=None):
        self.vault_id = vault_id
        self.block_id = block_id
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
            self.vault_id, self.block_id)

    def get_block_length(self):
        return deuce.storage_driver.get_block_object_length(
            self.vault_id, self.block_id)
