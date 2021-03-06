from deuce import conf

from deuce.drivers.blockstoragedriver import BlockStorageDriver

import hashlib
import importlib


from deuce.util import log

logger = log.getLogger(__name__)
from swiftclient.exceptions import ClientException

from six import BytesIO

import deuce


class SwiftStorageDriver(BlockStorageDriver):

    def __init__(self):
        self.lib_pack = importlib.import_module(
            conf.block_storage_driver.swift.swift_module)
        self.Conn = getattr(self.lib_pack, 'client')

    # =========== VAULTS ===============================

    def create_vault(self, vault_id):
        try:
            response = dict()
            self.Conn.put_container(
                url=deuce.context.openstack.swift.storage_url,
                token=deuce.context.openstack.auth_token,
                container=vault_id,
                response_dict=response)
            return response['status'] == 201
        except ClientException:
            return False

    def vault_exists(self, vault_id):
        try:

            response = self.Conn.head_container(
                url=deuce.context.openstack.swift.storage_url,
                token=deuce.context.openstack.auth_token,
                container=vault_id)

            return response

        except ClientException:
            return False

    def delete_vault(self, vault_id):
        try:
            response = dict()
            self.Conn.delete_container(
                url=deuce.context.openstack.swift.storage_url,
                token=deuce.context.openstack.auth_token,
                container=vault_id,
                response_dict=response)
            return response['status'] >= 200 and response['status'] < 300
        except ClientException:
            return False

    def get_vault_statistics(self, vault_id):
        """Return the statistics on the vault.

        "param vault_id: The ID of the vault to gather statistics for"""

        statistics = dict()
        statistics['internal'] = {}
        statistics['total-size'] = 0
        statistics['block-count'] = 0

        try:
            # This will always return a dictionary
            container_metadata = self.Conn.head_container(
                url=deuce.context.openstack.swift.storage_url,
                token=deuce.context.openstack.auth_token,
                container=vault_id)
            try:
                statistics['total-size'] = \
                    int(container_metadata['x-container-bytes-used'])
            except KeyError:  # pragma: no cover
                pass
            try:
                statistics['block-count'] = \
                    int(container_metadata['x-container-object-count'])
            except KeyError:  # pragma: no cover
                pass
            try:
                statistics['internal']['last-modification-time'] = \
                    container_metadata['x-timestamp']
            except KeyError:  # pragma: no cover
                statistics['internal']['last-modification-time'] = 0

        except ClientException as e:
            pass

        return statistics

    def get_vault_block_list(self, vault_id, limit, marker=None):
        try:
            container_block_list = self.Conn.get_container(
                url=deuce.context.openstack.swift.storage_url,
                token=deuce.context.openstack.auth_token,
                container=vault_id,
                limit=limit,
                marker=marker
            )
            return container_block_list
        except ClientException as e:
            return None

    # =========== BLOCKS ===============================

    def store_block(self, vault_id, metadata_block_id, blockdata):
        try:
            response = dict()
            mdhash = hashlib.md5()

            mdhash.update(blockdata)
            mdetag = mdhash.hexdigest()
            storage_id = self.storage_id(metadata_block_id)
            ret_etag = self.Conn.put_object(
                url=deuce.context.openstack.swift.storage_url,
                token=deuce.context.openstack.auth_token,
                container=vault_id,
                name=storage_id,
                contents=blockdata,
                content_length=str(len(blockdata)),
                etag=mdetag,
                response_dict=response)
            return (response['status'] == 201
                    and ret_etag == mdetag, storage_id)
        except ClientException:
            return (False, '')

    def store_async_block(self, vault_id, metadata_block_ids, blockdatas):
        try:
            response = dict()
            storage_ids = [self.storage_id(metadata_block_id)
                           for metadata_block_id in metadata_block_ids]
            rets = self.Conn.put_async_object(
                url=deuce.context.openstack.swift.storage_url,
                token=deuce.context.openstack.auth_token,
                container=vault_id,
                names=storage_ids,
                contents=blockdatas,
                etag=True,
                response_dict=response)
            return (response['status'] == 201, storage_ids)
        except ClientException:
            return (False, [])

    def block_exists(self, vault_id, storage_block_id):

        try:
            response = self.Conn.head_object(
                url=deuce.context.openstack.swift.storage_url,
                token=deuce.context.openstack.auth_token,
                container=vault_id,
                name=str(storage_block_id))

            return response

        except ClientException:
            return False

    def delete_block(self, vault_id, storage_block_id):

        response = dict()

        try:
            self.Conn.delete_object(
                url=deuce.context.openstack.swift.storage_url,
                token=deuce.context.openstack.auth_token,
                container=vault_id,
                name=str(storage_block_id),
                response_dict=response)
            return response['status'] >= 200 and response['status'] < 300
        except ClientException:
            return False

    def get_block_obj(self, vault_id, storage_block_id):

        try:
            buff = BytesIO()
            response = dict()
            # NOTE(TheSriram): block is a tuple of
            # headers and response body.
            block = self.Conn.get_object(
                url=deuce.context.openstack.swift.storage_url,
                token=deuce.context.openstack.auth_token,
                container=vault_id,
                name=str(storage_block_id),
                response_dict=response)

            if block[1]:
                buff.write(block[1])
                buff.seek(0)
                return buff
            else:
                return None
        except ClientException:
            return None

    def get_block_object_length(self, vault_id, storage_block_id):
        """Returns the length of an object"""
        response = dict()
        try:
            # NOTE(TheSriram): block is a tuple of
            # headers and response body
            block = \
                self.Conn.get_object(
                    url=deuce.context.openstack.swift.storage_url,
                    token=deuce.context.openstack.auth_token,
                    container=vault_id,
                    name=str(storage_block_id),
                    response_dict=response)

            return len(block[1])

        except ClientException:
            return 0
