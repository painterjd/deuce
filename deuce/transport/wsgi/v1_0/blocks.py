import json

import falcon
import msgpack

from deuce import conf
from deuce.util import set_qs_on_url
from deuce.model import Vault
from deuce.drivers.metadatadriver import ConstraintError
from deuce.transport.validation import *
import deuce.transport.wsgi.errors as errors
import deuce.util.log as logging

logger = logging.getLogger(__name__)


class ItemResource(object):

    @validate(vault_id=VaultGetRule, block_id=BlockGetRule)
    def on_get(self, req, resp, vault_id, block_id):
        """Returns a specific block"""

        # Step 1: Is the block in our vault store?  If not, return 404
        # Step 2: Stream the block back to the user
        vault = Vault.get(vault_id)

        # Existence of the vault should have been confirmed
        # in the vault controller
        assert vault is not None

        block = vault.get_block(block_id)

        if block is None:
            logger.error('block [{0}] does not exist'.format(block_id))
            raise errors.HTTPNotFound

        ref_cnt = block.get_ref_count()
        resp.set_header('X-Block-Reference-Count', str(ref_cnt))

        ref_mod = block.get_ref_modified()
        resp.set_header('X-Ref-Modified', str(ref_mod))

        resp.stream = block.get_obj()
        resp.stream_len = vault.get_block_length(block_id)
        resp.status = falcon.HTTP_200
        resp.content_type = 'application/octet-stream'

    @validate(vault_id=VaultPutRule, block_id=BlockPutRuleNoneOk)
    def on_put(self, req, resp, vault_id, block_id):
        """Uploads a block into Deuce. The URL of the block
        is returned in the Location header
        """

        vault = Vault.get(vault_id)

        try:
            retval = vault.put_block(
                block_id, req.stream.read(), req.get_header('content-length'))
            resp.status = (
                falcon.HTTP_201 if retval is True else falcon.HTTP_500)
            logger.info('block [{0}] added'.format(block_id))
        except ValueError as e:
            raise errors.HTTPPreconditionFailed('hash error')

    @validate(vault_id=VaultGetRule, block_id=BlockGetRule)
    def on_delete(self, req, resp, vault_id, block_id):
        """Unregisters a block_id from a given vault_id in
        the storage and metadata
        """
        vault = Vault.get(vault_id)

        if not vault:
            logger.error('Vault [{0}] does not exist'.format(vault_id))
            resp.status = falcon.HTTP_404
            return

        try:
            response = vault.delete_block(vault_id, block_id)

        except ConstraintError as ex:
            logger.error(json.dumps(ex.args))
            raise errors.HTTPPreconditionFailed(json.dumps(ex.args))

        except Exception as ex:  # pragma: no cover
            logger.error(ex)
            raise errors.HTTPServiceUnavailable

        else:

            if response:
                logger.info('block [{0}] deleted from vault {1}'
                            .format(block_id, vault_id))
                resp.status = falcon.HTTP_204

            else:
                logger.error('block [{0}] does not exist'.format(block_id))
                raise errors.HTTPNotFound


class CollectionResource(object):

    @validate(vault_id=VaultGetRule)
    def on_post(self, req, resp, vault_id):
        vault = Vault.get(vault_id)
        try:
            unpacked = msgpack.unpackb(req.stream.read())

            if not isinstance(unpacked, dict):
                raise TypeError

            else:
                block_ids = list(unpacked.keys())
                block_datas = list(unpacked.values())
                try:
                    retval = vault.put_async_block(
                        block_ids,
                        block_datas)
                    if retval:
                        resp.status = falcon.HTTP_201
                    else:
                        raise errors.HTTPInternalServerError('Block '
                                                            'Post Failed')
                    logger.info('blocks [{0}] added'.format(block_ids))
                except ValueError:
                    raise errors.HTTPPreconditionFailed('hash error')
        except (TypeError, ValueError):
            logger.error('Request Body not well formed '
                         'for posting muliple blocks to {0}'.format(vault_id))
            raise errors.HTTPBadRequestBody("Request Body not well formed")

    @validate(vault_id=VaultGetRule)
    @BlockMarkerRule
    @LimitRule
    def on_get(self, req, resp, vault_id):

        vault = Vault.get(vault_id)
        if not vault:
            logger.error('Vault [{0}] does not exist'.format(vault_id))
            raise errors.HTTPNotFound
        # NOTE(TheSriram): get_param(param) automatically returns None
        # if param is not present
        inmarker = req.get_param('marker')
        limit = req.get_param_as_int('limit') if req.get_param_as_int('limit') else \
            conf.api_configuration.max_returned_num

        # We actually fetch the user's requested
        # limit +1 to detect if the list is being
        # truncated or not.
        blocks = vault.get_blocks(inmarker, limit + 1)

        # List the blocks into JSON and return.
        # TODO: figure out a way to stream this back(??)
        responses = list(blocks)

        # Was the list truncated? See note above about +1
        truncated = len(responses) > 0 and len(responses) == limit + 1

        outmarker = responses.pop().block_id if truncated else None

        if outmarker:
            query_args = {'marker': outmarker}
            query_args['limit'] = limit
            returl = set_qs_on_url(req.url, query_args)
            resp.set_header("X-Next-Batch", returl)

        resp.body = json.dumps([response.block_id for response in responses])
