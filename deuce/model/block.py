
import deuce

from pecan import conf
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

    def __json__(self):
        return self.block_id
