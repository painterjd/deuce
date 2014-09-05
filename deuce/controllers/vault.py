from deuce.util import log as logging

from pecan import conf, expose, response, request
from pecan.rest import RestController

import deuce

from deuce.controllers.blocks import BlocksController
from deuce.controllers.files import FilesController
from deuce.controllers.validation import *
from deuce.model import Vault

from deuce.util import set_qs
import six
from six.moves.urllib.parse import urlparse, parse_qs


logger = logging.getLogger(__name__)


class VaultController(RestController):

    blocks = BlocksController()
    files = FilesController()

    @validate(marker=VaultMarkerRule, limit=LimitRule)
    @expose('json')
    def index(self):
        inmarker = request.params.get('marker')
        limit = int(request.params.get('limit',
            conf.api_configuration.max_returned_num))

        vaultlist = Vault.get_vaults_generator(
            inmarker, limit + 1)
        resp = list(vaultlist)

        if not resp:
            return list()

        # Note: the list may not actually be truncated
        truncated = len(resp) == limit + 1

        outmarker = resp.pop() if truncated else None

        # Set x-next-batch resp header.
        if outmarker:
            query_args = {'marker': outmarker}
            query_args['limit'] = limit
            returl = set_qs(request.url, query_args)
            response.headers["X-Next-Batch"] = returl

        # Set return json for vault URLs.
        p = urlparse(request.url)

        return dict(six.moves.map(lambda vaultname:
            (vaultname, {"url": p.scheme +
                '://' + p.netloc + p.path + vaultname}), resp))

    @validate(vault_name=VaultPutRule)
    @expose()
    def put(self, vault_name):
        vault = Vault.create(vault_name)
        # TODO: Need check and monitor failed vault.
        logger.info('Vault [{0}] created'.format(vault_name))
        response.status_code = 201 if vault else 500

    @validate(vault_id=VaultGetRule)
    @expose()
    def head(self, vault_id):
        """Returns the vault controller object"""

        if Vault.get(vault_id):
            # weblint complains about the content-type header being
            # present as pecan doesn't intelligently add it or remove
            # it.
            del response.headers["Content-Type"]
            response.status_code = 204
            return response
        else:
            logger.error('Vault [{0}] does not exist'.format(vault_id))
            response.status_code = 404

    @validate(vault_id=VaultGetRule)
    @expose('json')
    def get_one(self, vault_id):
        """Returns the statistics on vault controller object"""
        vault = Vault.get(vault_id)

        if vault:
            response.status_code = 200
            return vault.get_vault_statistics()
        else:
            logger.error('Vault [{0}] does not exist'.format(vault_id))
            response.status_code = 404
            return None

    @validate(vault_id=VaultPutRule)
    @expose()
    def delete(self, vault_id):
        vault = Vault.get(vault_id)

        if vault:
            if vault.delete():
                logger.info('Vault [{0}] deleted'.format(vault_id))
                # weblint complains about the content-type header being present
                # as pecan doesn't intelligently add it or remove it.
                del response.headers["Content-Type"]
                response.status_code = 204

            else:
                logger.info('Vault [{0}] cannot be deleted'.format(vault_id))
                response.status_code = 412

        else:
            logger.error('Vault [{0}] deletion failed; '
                'Vault does not exist'.format(vault_id))
            response.status_code = 404
