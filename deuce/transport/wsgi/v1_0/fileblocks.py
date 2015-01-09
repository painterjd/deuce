import json

from stoplight import validate

from deuce.util import set_qs_on_url
from deuce.model import Vault
from deuce import conf
import deuce.util.log as logging
from deuce.transport.validation import *
import deuce

logger = logging.getLogger(__name__)


class CollectionResource(object):

    @validate(req=RequestRule(OffsetMarkerRule, LimitRule),
              vault_id=VaultGetRule, file_id=FileGetRule)
    def on_get(self, req, resp, vault_id, file_id):

        vault = Vault.get(vault_id)

        assert vault is not None

        f = vault.get_file(file_id)

        if not f:
            logger.error('File [{0}] does not exist'.format(file_id))
            raise errors.HTTPNotFound
        # NOTE(TheSriram): get_param(param) automatically returns None
        # if param is not present
        inmarker = req.get_param_as_int('marker')
        limit = req.get_param_as_int('limit') if req.get_param_as_int('limit') \
            else conf.api_configuration.default_returned_num

        # Get the block generator from the metadata driver.
        # Note: +1 on limit is to fetch one past the limt
        # for the purpose of determining if the
        # list was truncated

        retblks = deuce.metadata_driver.create_file_block_generator(
            vault_id, file_id, inmarker, limit + 1)

        responses = list(retblks)

        truncated = len(responses) > 0 and len(responses) == limit + 1
        outmarker = responses.pop()[1] if truncated else None

        if outmarker:
            query_args = {'marker': outmarker}
            query_args['limit'] = limit

            returl = set_qs_on_url(req.url, query_args)
            resp.set_header("X-Next-Batch", returl)

        resp.body = json.dumps(responses)

    @validate(vault_id=VaultPutRule, file_id=FilePostRuleNoneOk)
    def on_post(self, req, resp, vault_id, file_id):
        """This endpoint Assigns blocks to files
        """
        vault = Vault.get(vault_id)

        # caller tried to post to a vault that
        # does not exist
        if not vault:
            logger.error('Vault [{0}] does not exist'.format(vault_id))
            raise errors.HTTPBadRequestAPI('Vault does not exist')

        f = vault.get_file(file_id)

        if not f:
            logger.error('File [{0}] does not exist'.format(file_id))
            raise errors.HTTPNotFound

        if f.finalized:
            logger.error('Finalized file [{0}] '
                         'cannot be modified'.format(file_id))
            raise errors.HTTPConflict('Finalized file cannot be modified')

        body = req.stream.read(req.content_length)
        # TODO (TheSriram): Validate payload
        payload = json.loads(body.decode())
        block_ids, offsets = zip(*payload)

        missing_blocks = deuce.metadata_driver.has_blocks(vault_id, block_ids)
        deuce.metadata_driver.assign_blocks(vault_id, file_id, block_ids,
                                            offsets)

        resp.body = json.dumps(missing_blocks)
