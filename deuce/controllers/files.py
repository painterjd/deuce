from deuce.util import log as logging

from pecan import conf, expose, request, response
from pecan.core import abort
from pecan.rest import RestController

import deuce
from deuce.controllers.fileblocks import FileBlocksController
from deuce.controllers.validation import *

from deuce.model import Vault, Block, File
from deuce.util import FileCat
from deuce.util import set_qs

logger = logging.getLogger(__name__)


# Standard rule for marker-limit semantics
# for the listing files


class FilesController(RestController):

    blocks = FileBlocksController()

    @validate(vault_id=VaultGetRule, file_id=FileGetRule)
    @expose('json')
    def delete(self, vault_id, file_id):

        vault = Vault.get(vault_id, request.auth_token)
        if not vault:
            abort(404)

        f = vault.get_file(file_id)
        if not f:
            abort(404)

        vault.delete_file(file_id)

    @validate(vault_id=VaultGetRule, marker=FileMarkerRule, limit=LimitRule)
    @expose('json')
    def get_all(self, vault_id):
        response.headers["Transaction-ID"] = request.context.request_id
        vault = Vault.get(vault_id, request.auth_token)

        if not vault:
            logger.error('Vault [{0}] does not exist'.format(vault_id))
            abort(404, headers={"Transaction-ID": request.context.request_id})

        inmarker = request.params.get('marker')
        limit = int(request.params.get('limit',
           conf.api_configuration.max_returned_num))

        # The +1 is to fetch one past the user's
        # requested limit so that we can determine
        # if the list was truncated or not
        files = vault.get_files(inmarker, limit + 1)

        resp = list(files)

        # Note: the list may not actually be truncated
        truncated = len(resp) == limit + 1

        outmarker = resp.pop().file_id if truncated else None

        if outmarker:
            query_args = {'marker': outmarker}
            query_args['limit'] = limit

            returl = set_qs(request.url, query_args)

            response.headers["X-Next-Batch"] = returl

        return resp

    @validate(vault_id=VaultGetRule, file_id=FileGetRule)
    @expose(content_type='application/octet-stream;')
    def get_one(self, vault_id, file_id):
        """Fetches, re-assembles and streams a single
        file out of Deuce"""
        response.headers["Transaction-ID"] = request.context.request_id
        vault = Vault.get(vault_id, request.auth_token)

        if not vault:
            logger.error('Vault [{0}] does not exist'.format(vault_id))
            abort(404, headers={"Transaction-ID": request.context.request_id})

        f = vault.get_file(file_id)

        if not f:
            logger.error('File [{0}] does not exist'.format(file_id))
            abort(404, headers={"Transaction-ID": request.context.request_id})

        if not f.finalized:
            abort(412, headers={"Transaction-ID": request.context.request_id})

        block_gen = deuce.metadata_driver.create_file_block_generator(
            vault_id, file_id)

        block_ids = [block[0] for block in sorted(block_gen,
            key=lambda block: block[1])]

        objs = vault.get_blocks_generator(block_ids,
            auth_token=request.auth_token)

        response.content_length = vault.get_file_length(file_id)
        response.body_file = FileCat(objs)
        response.status_code = 200

    @validate(vault_id=VaultPutRule, file_id=FilePostRuleNoneOk)
    @expose('json')
    def post(self, vault_id, file_id=None):
        """Initializes a new file. The location of
        the new file is returned in the Location
        header
        """
        response.headers["Transaction-ID"] = request.context.request_id
        vault = Vault.get(vault_id, request.auth_token)

        # caller tried to post to a vault that
        # does not exist
        if not vault:
            logger.error('Vault [{0}] does not exist'.format(vault_id))
            abort(400, headers={"Transaction-ID": request.context.request_id})

        # overload to use the same end-point for creating a new file
        # and assigning blocks to a file that is in progress
        if file_id is not None:
            return self._assign(vault, vault_id, file_id)

        file = vault.create_file()

        response.headers["Location"] = "files/%s" % file.file_id
        response.status_code = 201  # Created
        logger.info('File [{0}] created'.
            format(response.headers["Location"]))

    def _assign(self, vault, vault_id, file_id):
        response.headers["Transaction-ID"] = request.context.request_id
        f = vault.get_file(file_id)

        if not f:
            logger.error('File [{0}] does not exist'.format(file_id))
            abort(404, headers={"Transaction-ID": request.context.request_id})

        if not request.body:
            try:
                # Fileid with an empty body will finalize the file.
                filesize = request.headers['Filesize'] if 'Filesize' \
                    in request.headers.keys() else 0
                res = deuce.metadata_driver.finalize_file(vault_id, file_id,
                    filesize)
                return res
            except Exception as e:
                # There are gaps or overlaps in blocks of the file
                # The list of errors returns
                # NEED RETURN 413
                details = str(e)
                response.status_code = 413
                logger.error('File [{0}] finalization '
                    'failed; [{1}]'.format(file_id, details))
                return details

        if f.finalized:
            # A finalized file cannot be
            # modified
            # TODO: Determine a better, more precise
            #       status code
            logger.error('Finalized file [{0}] '
                'cannot be modified'.format(file_id))
            abort(400, headers={"Transaction-ID": request.context.request_id})

        blocks = request.json_body['blocks']

        missing_blocks = list()

        for mapping in blocks:

            block_id = mapping['id']
            offset = int(mapping['offset'])

            if not deuce.metadata_driver.has_block(vault_id, block_id):

                missing_blocks.append(block_id)

            deuce.metadata_driver.assign_block(vault_id, file_id,
                mapping['id'], mapping['offset'])

        return missing_blocks
