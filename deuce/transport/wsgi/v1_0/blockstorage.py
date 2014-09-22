import json

import falcon
import msgpack

from deuce import conf
from deuce.drivers.metadatadriver import ConstraintError
from deuce.model import Vault
from deuce.transport.validation import *
import deuce.transport.wsgi.errors as errors
from deuce.util import set_qs_on_url
import deuce.util.log as logging

logger = logging.getLogger(__name__)


class ItemResource(object):

    @validate(vault_id=VaultPutRule, block_id=BlockPutRuleNoneOk)
    def on_put(self, req, resp, vault_id, block_id):
        """Note: This does not support PUT as it is read-only + DELETE
        """
        url = req.uri
        url.replace('blockstorage', 'block')
        raise errors.HTTPMethodNotAllowed(
            ['HEAD', 'GET', 'DELETE'],
            'This is read-only access. Uploads must go to {0:}'.format(
                url))

    @validate(vault_id=VaultGetRule, block_id=BlockGetRule)
    def on_get(self, req, resp, vault_id, block_id):
        """Returns a specific block from storage alone"""
        raise errors.HTTPNotImplemented(
            'Get Block Direct From Storage Not Implemented')

    @validate(vault_id=VaultGetRule, block_id=BlockGetRule)
    def on_head(self, req, resp, vault_id, block_id):
        """Returns the block data from storage alone"""
        raise errors.HTTPNotImplemented(
            'Head Block In Storage Directly Not Implemented')

    @validate(vault_id=VaultGetRule, block_id=BlockGetRule)
    def on_delete(self, req, resp, vault_id, block_id):
        """Deletes a block_id from a given vault_id in
        the storage after verifying it does not exist
        in metadata (due diligence applies)
        """
        raise errors.HTTPNotImplemented(
            'Directly Delete Block From Storage Not Implemented')


class CollectionResource(object):

    @validate(vault_id=VaultGetRule)
    @BlockMarkerRule
    @LimitRule
    def on_get(self, req, resp, vault_id):
        """List the blocks in the vault from storage-alone
        """
        raise errors.HTTPNotImplemented(
            'List Blocks Directly From Storage Not Implemented')
