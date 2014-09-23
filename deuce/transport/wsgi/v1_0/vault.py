import falcon
import six
from six.moves.urllib.parse import urlparse, parse_qs
from deuce.util import set_qs_on_url
from deuce.model import Vault
from deuce import conf
import deuce.util.log as logging
from deuce.transport.validation import *
import deuce.transport.wsgi.errors as errors
logger = logging.getLogger(__name__)
import json


class ItemResource(object):

    @validate(vault_id=VaultGetRule)
    def on_get(self, req, resp, vault_id):
        """Returns the statistics on vault controller object"""
        vault = Vault.get(vault_id)

        if vault:
            resp.body = json.dumps(vault.get_vault_statistics())
            resp.status = falcon.HTTP_200
        else:
            logger.error('Vault [{0}] does not exist'.format(vault_id))
            raise errors.HTTPNotFound

    @validate(vault_id=VaultGetRule)
    def on_head(self, req, resp, vault_id):
        """Returns the vault controller object"""

        if Vault.get(vault_id):
            resp.status = falcon.HTTP_204

        else:
            logger.error('Vault [{0}] does not exist'.format(vault_id))
            raise errors.HTTPNotFound

    @validate(vault_id=VaultPutRule)
    def on_put(self, req, resp, vault_id):

        vault = Vault.create(vault_id)
        # TODO: Need check and monitor failed vault.
        logger.info('Vault [{0}] created'.format(vault_id))
        if vault:
            resp.status = falcon.HTTP_201
        else:
            raise errors.HTTPInternalServerError('Vault Creation Failed')

    @validate(vault_id=VaultPutRule)
    def on_delete(self, req, resp, vault_id):
        vault = Vault.get(vault_id)

        if vault:
            if vault.delete():
                logger.info('Vault [{0}] deleted'.format(vault_id))
                resp.status = falcon.HTTP_204

            else:
                logger.info('Vault [{0}] cannot be deleted'.format(vault_id))
                raise errors.HTTPPreconditionFailed('Vault cannot be deleted')

        else:
            logger.error('Vault [{0}] deletion failed; '
                         'Vault does not exist'.format(vault_id))
            raise errors.HTTPNotFound


class CollectionResource(object):

    @VaultMarkerRule
    @LimitRule
    def on_get(self, req, resp):

        # NOTE(TheSriram): get_param(param) automatically returns None
        # if param is not present
        inmarker = req.get_param('marker')
        limit = req.get_param_as_int('limit') if req.get_param_as_int('limit') else \
            conf.api_configuration.max_returned_num

        vaultlist = Vault.get_vaults_generator(
            inmarker, limit + 1)
        response = list(vaultlist)

        if not response:
            resp.body = json.dumps([])

        # Note: the list may not actually be truncated
        truncated = len(response) == limit + 1

        outmarker = response.pop() if truncated else None

        # Set x-next-batch resp header.
        if outmarker:
            query_args = {'marker': outmarker}
            query_args['limit'] = limit
            returl = set_qs_on_url(req.url, query_args)
            resp.set_header(name="X-Next-Batch", value=returl)

        # Set return json for vault URLs.
        p = urlparse(req.url)
        resp.body = json.dumps(dict(six.moves.map(lambda vaultname:
            (vaultname, {"url": p.scheme +
                '://' + p.netloc + p.path + '/' + vaultname}), response)))
