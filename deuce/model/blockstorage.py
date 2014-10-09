import deuce
from deuce.model import Vault
from deuce.util import log as logging
import deuce.transport.wsgi.errors as errors

logger = logging.getLogger(__name__)


class BlockStorage(object):

    def __init__(self, vault_id):
        self.vault_id = vault_id
        self.Vault = Vault.get(vault_id)

    def delete_block(self, block_id):
        # TODO: Implement this, error from here is just temporary
        raise errors.HTTPNotImplemented(
            'Directly Delete Block From Storage Not Implemented')
        # return False

    def head_block(self, block_id):
        # TODO: Implement this, error from here is just temporary
        raise errors.HTTPNotImplemented(
            'Directly Delete Block From Storage Not Implemented')
        # return {'x': 'y'}

    def get_block(self, block_id):
        # TODO: Implement this, error from here is just temporary
        raise errors.HTTPNotImplemented(
            'Directly Delete Block From Storage Not Implemented')
        # return False

    @staticmethod
    def get_blocks_generator(marker, limit):
        # TODO: Implement this, error from here is just temporary
        raise errors.HTTPNotImplemented(
            'Directly Delete Block From Storage Not Implemented')
        # return []
