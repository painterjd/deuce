from deuce.util import log as logging

from pecan import expose, response, request
from pecan.rest import RestController

import deuce

from deuce.controllers.blocks import BlocksController
from deuce.controllers.files import FilesController
from deuce.controllers.validation import *
from deuce.model import Vault

logger = logging.getLogger(__name__)


class VaultController(RestController):

    blocks = BlocksController()
    files = FilesController()

    @expose()
    def index(self):
        logger.warning('Invalid vault controller index request')
        response.status_code = 404

    @validate(vault_name=VaultPutRule)
    @expose()
    def put(self, vault_name):
        response.headers["Transaction-ID"] = request.context.request_id
        vault = Vault.create(vault_name, request.auth_token)
        # TODO: Need check and monitor failed vault.
        logger.info('Vault [{0}] created'.format(vault_name))
        response.status_code = 201 if vault else 500

    @validate(vault_id=VaultGetRule)
    @expose()
    def head(self, vault_id):
        """Returns the vault controller object"""

        response.headers["Transaction-ID"] = request.context.request_id
        if Vault.get(vault_id, request.auth_token):
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
        response.headers["Transaction-ID"] = request.context.request_id

        vault = Vault.get(vault_id, request.auth_token)

        if vault:
            response.status_code = 200
            return vault.get_vault_statistics(request.auth_token)
        else:
            logger.error('Vault [{0}] does not exist'.format(vault_id))
            response.status_code = 404
            return None

    @validate(vault_id=VaultPutRule)
    @expose()
    def delete(self, vault_id):
        response.headers["Transaction-ID"] = request.context.request_id
        vault = Vault.get(vault_id, request.auth_token)

        if vault:
            if vault.delete(request.auth_token):
                logger.info('Vault [{0}] deleted'.format(vault_id))
                del response.headers["Content-Type"]
                response.status_code = 204

            else:
                logger.info('Vault [{0}] cannot be deleted'.format(vault_id))
                response.status_code = 412

        else:
            logger.error('Vault [{0}] deletion failed; '
                'Vault does not exist'.format(vault_id))
            response.status_code = 404
