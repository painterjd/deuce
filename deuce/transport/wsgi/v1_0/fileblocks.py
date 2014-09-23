import json

import falcon

from deuce.util import set_qs_on_url
from deuce.model import Vault
from deuce import conf
import deuce.util.log as logging
from deuce.transport.validation import *
import deuce

logger = logging.getLogger(__name__)


class CollectionResource(object):

    @validate(vault_id=VaultGetRule, file_id=FileGetRule)
    @OffsetMarkerRule
    @LimitRule
    def on_get(self, req, resp, vault_id, file_id):

        vault = Vault.get(vault_id)

        assert vault is not None

        f = vault.get_file(file_id)

        if not f:
            logger.error('File [{0}] does not exist'.format(file_id))
            raise errors.HTTPNotFound
        # NOTE(TheSriram): get_param(param) automatically returns None
        # if param is not present
        inmarker = req.get_param('marker')
        limit = req.get_param_as_int('limit') if req.get_param_as_int('limit') \
            else conf.api_configuration.max_returned_num

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
