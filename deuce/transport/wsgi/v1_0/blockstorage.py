import json

import falcon
import msgpack
import six
from six.moves.urllib.parse import urlparse, parse_qs
from stoplight import validate

import deuce
from deuce import conf
from deuce.drivers.metadatadriver import ConstraintError
from deuce.model import BlockStorage, Vault
from deuce.transport.validation import *
import deuce.transport.wsgi.errors as errors
from deuce.util import set_qs_on_url
import deuce.util.log as logging

logger = logging.getLogger(__name__)


class ItemResource(object):

    @validate(vault_id=VaultPutRule, storage_block_id=StorageBlockPutRule)
    def on_put(self, req, resp, vault_id, storage_block_id):
        """Note: This does not support PUT as it is read-only + DELETE
        """
        # PUT operations must go to /vaults/{vaultid}/blocks
        # instead of /vaults/{vaultid}/storage/blocks
        path = req.path

        path_parts = path.split('/')
        del path_parts[4]

        # If there exists a Block ID in Metadata then remove
        # Storage Block ID and insert Metadata Block ID
        # Otherwise, assume the block_id is the what the Blocks
        # PUT requires
        metadata_block_id = deuce.metadata_driver.get_block_metadata_id(
            vault_id, storage_block_id)

        if metadata_block_id is not None:
            del path_parts[(len(path_parts) - 1)]
            path_parts.append(metadata_block_id)

        resp.set_header('X-Block-ID', metadata_block_id)
        resp.set_header('X-Storage-ID', storage_block_id)

        path = str('/').join(path_parts)

        block_url = (req.protocol + '://' +
                     req.get_header('host') +
                     req.app +
                     path)

        resp.set_header('X-Block-Location', block_url)

        logger.warn('Caller tried to PUT a block directly to storage. '
            'Transaction: {0} Project: {1}'.format(
                deuce.context.transaction.request_id,
                deuce.context.project_id))
        raise errors.HTTPMethodNotAllowed(
            ['HEAD', 'GET', 'DELETE'],
            'This is read-only access. Uploads must go to {0:}'.format(
                block_url))

    @validate(vault_id=VaultGetRule, storage_block_id=StorageBlockGetRule)
    def on_get(self, req, resp, vault_id, storage_block_id):
        """Returns a specific block from storage alone"""
        storage = BlockStorage(vault_id)

        assert storage is not None

        block = storage.get_block(storage_block_id)
        # TODO: Finish this

    @validate(vault_id=VaultGetRule, storage_block_id=StorageBlockGetRule)
    def on_head(self, req, resp, vault_id, storage_block_id):
        """Returns the block data from storage alone"""
        storage = BlockStorage(vault_id)

        assert storage is not None

        storage_block_info = storage.head_block(storage_block_id)
        # for k in storage_block_info.keys():
        #    resp.set_headers(k, storage_block_info[k])

        # resp.status = falcon.HTTP_204

    @validate(vault_id=VaultGetRule, storage_block_id=StorageBlockGetRule)
    def on_delete(self, req, resp, vault_id, storage_block_id):
        """Deletes a storage_block_id from a given vault_id in
        the storage after verifying it does not exist
        in metadata (due diligence applies)
        """
        storage = BlockStorage(vault_id)

        assert storage is not None

        block = storage.delete_block(storage_block_id)
        # resp.status = falcon.HTTP_204


class CollectionResource(object):

    @validate(req=RequestRule(StorageBlockMarkerRule, LimitRule),
              vault_id=VaultGetRule)
    def on_get(self, req, resp, vault_id):
        """List the blocks in the vault from storage-alone
        """
        vault = Vault.get(vault_id)
        if not vault:
            logger.error('Vault [{0}] does not exist'.format(vault_id))
            raise errors.HTTPNotFound

        inmarker = req.get_param('marker') if req.get_param('marker') else None
        limit = req.get_param_as_int('limit') if req.get_param_as_int('limit') else \
            conf.api_configuration.max_returned_num

        # We actually fetch the user's requested
        # limit +1 to detect if the list is being
        # truncated or not.
        storage_blocks = BlockStorage.get_blocks_generator(inmarker, limit + 1)

        # List the blocks into JSON and return.
        # TODO: figure out a way to stream this back(??)
        # responses = list(storage_blocks)

        # Was the list truncated? See note above about +1
        # truncated = len(responses) > 0 and len(responses) == limit + 1

        # outmarker = responses.pop().storage_block_id if truncated else None

        # if outmarker:
        #    query_args = {'marker': outmarker}
        #    query_args['limit'] = limit
        #    returl = set_qs_on_url(req.url, query_args)
        #    resp.set_header("X-Next-Batch", returl)

        # resp.body = json.dumps([response.storage_block_id
        #                         for response in responses])
