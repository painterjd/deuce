

import deuce
from deuce.util import log as logging


logger = logging.getLogger(__name__)


class BlockStorage(object):

    def __init__(self, vault_id):
        self.vault_id = vault_id

    def delete_block(self, block_id):
        pass

    def head_block(self, block_id):
        pass

    def get_block(self, block_id):
        pass

    def get_blocks(self, marker, limit):
        pass

    def list_blocks(self, marker, limit):
        pass
