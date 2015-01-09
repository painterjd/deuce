import json
from stoplight import validate
import falcon
import msgpack

import deuce
from deuce import conf
from deuce.util import set_qs_on_url
from deuce.model import Vault
from deuce.model import Block
from deuce.model.exceptions import ConsistencyError
from deuce.drivers.metadatadriver import ConstraintError
from deuce.transport.validation import *

import deuce.transport.wsgi.errors as errors
import deuce.util.log as logging

logger = logging.getLogger(__name__)


class ItemResource(object):
    @validate(vault_id=VaultGetRule, block_id=BlockGetRule)
    def on_head(self, req, resp, vault_id, block_id):

        """Checks for the existence of the block in the
        metadata storage and if successful check for it in
        the storage driver
        if it fails we return a 502, otherwise we return
        all other headers returned on
            GET /v1.0/vaults/{vault_id}/blocks/{block_id}
        """

        vault = Vault.get(vault_id)

        if not vault:
            logger.error('Vault [{0}] does not exist'.format(vault_id))
            raise errors.HTTPNotFound

        # This is safe to construct before we check the status of the block
        # b/c the constructor does not do anything other than save values
        # to itself, no lookups, etc
        block = Block(vault_id, block_id)
        try:
            if not vault.has_block(block_id, check_storage=True):
                logger.error('block [{0}] does not exist'.format(block_id))
                raise errors.HTTPNotFound
            ref_cnt = block.get_ref_count()
            resp.set_header('X-Block-Reference-Count', str(ref_cnt))

            ref_mod = block.get_ref_modified()
            resp.set_header('X-Ref-Modified', str(ref_mod))

            storage_id = block.get_storage_id()
            resp.set_header('X-Storage-ID', str(storage_id))
            resp.set_header('X-Block-ID', str(block_id))

            resp.set_header('X-Block-Size',
                            str(block.get_block_length()))

            resp.status = falcon.HTTP_204

        except ConsistencyError as ex:
            # We have the block in metadata...
            # so we can get anything that only touches metadata
            ref_cnt = block.get_ref_count()
            resp.set_header('X-Block-Reference-Count', str(ref_cnt))

            ref_mod = block.get_ref_modified()
            resp.set_header('X-Ref-Modified', str(ref_mod))

            storage_id = block.get_storage_id()
            resp.set_header('X-Storage-ID', str(storage_id))
            resp.set_header('X-Block-ID', str(block_id))

            # Block-size is retrieved from storage...

            logger.error(ex)
            raise errors.HTTPGone(str(ex))

    @validate(vault_id=VaultGetRule, block_id=BlockGetRule)
    def on_get(self, req, resp, vault_id, block_id):
        """Returns a specific block"""

        # Step 1: Is the block in our vault store?  If not, return 404
        # Step 2: Stream the block back to the user
        vault = Vault.get(vault_id)

        # Existence of the vault should have been confirmed
        # in the vault controller
        assert vault is not None

        try:
            block = vault.get_block(block_id)

            if block is None:
                logger.error('block [{0}] does not exist'
                             .format(block_id))

                # We have to do the has_block() check in order to
                # differentiate between a 404 and 410 error.
                # 404 should be returned if even metadata doesn't know
                # about the block; while 410 should be returned if
                # metadata knows about the block but it is not found
                # in storage. Since we already know the block doesn't
                # exist in storage, we can skip the storage check
                if vault.has_block(block_id, check_storage=False):
                    logger.error('block [{0}] does not exist (vault check)'
                                 .format(block_id))
                    raise ConsistencyError(deuce.context.project_id,
                                           vault_id, block_id,
                                           msg='Block does not exist'
                                               ' in Block Storage')

                raise errors.HTTPNotFound

            ref_cnt = block.get_ref_count()
            resp.set_header('X-Block-Reference-Count', str(ref_cnt))

            ref_mod = block.get_ref_modified()
            resp.set_header('X-Ref-Modified', str(ref_mod))

            storage_id = block.get_storage_id()
            resp.set_header('X-Storage-ID', str(storage_id))
            resp.set_header('X-Block-ID', str(block_id))

            resp.stream = block.get_obj()
            resp.stream_len = block.get_block_length()

            resp.status = falcon.HTTP_200
            resp.content_type = 'application/octet-stream'

        except ConsistencyError as ex:
            # We have the block in metadata...
            # so we can get anything that only touches metadata
            block = Block(vault_id, block_id)

            ref_cnt = block.get_ref_count()
            resp.set_header('X-Block-Reference-Count', str(ref_cnt))

            ref_mod = block.get_ref_modified()
            resp.set_header('X-Ref-Modified', str(ref_mod))

            storage_id = block.get_storage_id()
            resp.set_header('X-Storage-ID', str(storage_id))
            resp.set_header('X-Block-ID', str(block_id))

            # Block-size is retrieved from storage...

            logger.error(ex)
            raise errors.HTTPGone(str(ex))

    @validate(vault_id=VaultPutRule, block_id=BlockPutRule)
    def on_put(self, req, resp, vault_id, block_id):
        """Uploads a block into Deuce. The URL of the block
        is returned in the Location header
        """

        vault = Vault.get(vault_id)

        try:
            retval, storage_id = vault.put_block(
                block_id, req.stream.read(), req.content_length)
            resp.set_header('X-Storage-ID', str(storage_id))
            resp.set_header('X-Block-ID', str(block_id))

            block = vault.get_block(block_id)

            ref_cnt = 0
            ref_mod = 0

            if retval:
                ref_cnt = block.get_ref_count()
                ref_mod = block.get_ref_modified()

            resp.set_header('X-Block-Reference-Count', str(ref_cnt))
            resp.set_header('X-Ref-Modified', str(ref_mod))

            resp.status = (
                falcon.HTTP_201 if retval is True else falcon.HTTP_500)
            logger.info('block [{0}] added [{1}]'.format(block_id, storage_id))
        except ValueError as e:
            raise errors.HTTPPreconditionFailed('hash error')
        except BufferError as e:
            raise errors.HTTPPreconditionFailed(
                'content length did not match data length')

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
            raise errors.HTTPConflict(json.dumps(ex.args))

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
                         'for posting multiple blocks to {0}'.format(vault_id))
            raise errors.HTTPBadRequestBody("Request Body not well formed")

    @validate(req=RequestRule(BlockMarkerRule, LimitRule),
              vault_id=VaultGetRule)
    def on_get(self, req, resp, vault_id):

        vault = Vault.get(vault_id)
        if not vault:
            logger.error('Vault [{0}] does not exist'.format(vault_id))
            raise errors.HTTPNotFound
        # NOTE(TheSriram): get_param(param) automatically returns None
        # if param is not present
        inmarker = req.get_param('marker')
        limit = req.get_param_as_int('limit') if req.get_param_as_int('limit') else \
            conf.api_configuration.default_returned_num

        # We actually fetch the user's requested
        # limit +1 to detect if the list is being
        # truncated or not.
        blocks = vault.get_blocks(inmarker, limit + 1)

        # List the blocks into JSON and return.
        # TODO: figure out a way to stream this back(??)
        responses = list(blocks)

        # Was the list truncated? See note above about +1
        truncated = len(responses) > 0 and len(responses) == limit + 1

        outmarker = responses.pop().metadata_block_id if truncated else None

        if outmarker:
            query_args = {'marker': outmarker}
            query_args['limit'] = limit
            returl = set_qs_on_url(req.url, query_args)
            resp.set_header("X-Next-Batch", returl)

        resp.body = json.dumps([response.metadata_block_id
                                for response in responses])
