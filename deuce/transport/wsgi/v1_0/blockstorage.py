import json

import falcon
import msgpack
import six
from six.moves.urllib.parse import urlparse, parse_qs

from deuce import conf
from deuce.drivers.metadatadriver import ConstraintError
from deuce.model import BlockStorage, Vault
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
        storage = BlockStorage(vault_id)

        assert storage is not None

        block = storage.get_block(block_id)
        # TODO: Finish this

    @validate(vault_id=VaultGetRule, block_id=BlockGetRule)
    def on_head(self, req, resp, vault_id, block_id):
        """Returns the block data from storage alone"""
        storage = BlockStorage(vault_id)

        assert storage is not None

        block_info = storage.head_block(block_id)
        # for k in block_info.keys():
        #    resp.set_headers(k, block_info[k])

        # resp.status = falcon.HTTP_204

    @validate(vault_id=VaultGetRule, block_id=BlockGetRule)
    def on_delete(self, req, resp, vault_id, block_id):
        """Deletes a block_id from a given vault_id in
        the storage after verifying it does not exist
        in metadata (due diligence applies)
        """
        storage = BlockStorage(vault_id)

        assert storage is not None

        block = storage.delete_block(block_id)
        # resp.status = falcon.HTTP_204


class CollectionResource(object):

    @validate(vault_id=VaultGetRule)
    @BlockMarkerRule
    @LimitRule
    def on_get(self, req, resp, vault_id):
        """List the blocks in the vault from storage-alone
        """
        vault = Vault.get(vault_id)
        if not vault:
            logger.error('Vault [{0}] does not exist'.format(vault_id))
            raise errors.HTTPNotFound

        inmarker = req.get_param('marker') if req.get_param('marker') else 0
        limit = req.get_param_as_int('limit') if req.get_param_as_int('limit') else \
            conf.api_configuration.max_returned_num

        # We actually fetch the user's requested
        # limit +1 to detect if the list is being
        # truncated or not.
        blocks = BlockStorage.get_blocks_generator(inmarker, limit + 1)

        # List the blocks into JSON and return.
        # TODO: figure out a way to stream this back(??)
        # responses = list(blocks)

        # Was the list truncated? See note above about +1
        # truncated = len(responses) > 0 and len(responses) == limit + 1

        # outmarker = responses.pop().block_id if truncated else None

        # if outmarker:
        #    query_args = {'marker': outmarker}
        #    query_args['limit'] = limit
        #    returl = set_qs_on_url(req.url, query_args)
        #    resp.set_header("X-Next-Batch", returl)

        # resp.body = json.dumps([response.block_id for response in responses])
