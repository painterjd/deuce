import ddt
import hashlib
import json
import falcon
import msgpack
import os
from random import randrange
from mock import patch
from deuce import conf
from deuce.util.misc import set_qs, relative_uri
from six.moves.urllib.parse import urlparse, parse_qs

from deuce.tests import ControllerTest


@ddt.ddt
class TestBlockStorageController(ControllerTest):

    def setUp(self):
        super(TestBlockStorageController, self).setUp()

        # Create a vault for us to work with

        self.vault_name = self.create_vault_id()
        self._vault_path = '/v1.0/vaults/{0}'.format(self.vault_name)
        self._blocks_path = '{0:}/blocks'.format(self._vault_path)
        self._storage_path = '{0:}/storage'.format(self._vault_path)
        self._block_storage_path = '{0:}/blocks'.format(self._storage_path)

        self._hdrs = {"x-project-id": self.create_project_id()}
        self.helper_create_vault(self.vault_name, self._hdrs)

    def tearDown(self):
        self.helper_delete_vault(self.vault_name, self._hdrs)
        super(TestBlockStorageController, self).tearDown()

    def get_storage_block_path(self, storage_block_id):
        return '{0:}/{1:}'.format(self._block_storage_path, storage_block_id)

    def get_block_path(self, block_id):
        return '{0:}/{1:}'.format(self._blocks_path, block_id)

    def test_put_block_nonexistant_block(self):
        # No block already in metadata/storage

        block_id = self.create_storage_block_id()

        block_path = self.get_storage_block_path(block_id)

        response = self.simulate_put(block_path,
                                     headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_405)
        self.assertTrue(self.srmock.headers_dict['x-block-location'])
        self.assertIn('x-storage-id', self.srmock.headers_dict)
        self.assertEqual(block_id, self.srmock.headers_dict['x-storage-id'])
        self.assertIn('x-block-id', self.srmock.headers_dict)
        self.assertIsNone(self.srmock.headers_dict['x-block-id'])

    def test_put_block_existing_block(self):
        # block already in metadata/storage

        # Generate a block
        upload_data = os.urandom(100)
        upload_block_id = self.calc_sha1(upload_data)

        # Upload it to Deuce in the correct method (via blocks/{sha1})
        upload_block_path = self.get_block_path(upload_block_id)
        upload_headers = self._hdrs
        upload_headers.update({
            "Content-Type": "application/octet-stream",
            "Content-Length": "100"
        })
        response = self.simulate_put(upload_block_path,
                                     headers=self._hdrs,
                                     body=upload_data)
        self.assertEqual(self.srmock.status, falcon.HTTP_201)
        self.assertIn('x-storage-id', self.srmock.headers_dict)
        self.assertIn('x-block-id', self.srmock.headers_dict)
        self.assertEqual(upload_block_id,
                         self.srmock.headers_dict['x-block-id'])

        # Now try to upload it via the Storage Blocks method
        storage_block_id = self.srmock.headers_dict['x-storage-id']
        block_path = self.get_storage_block_path(
            storage_block_id)

        response = self.simulate_put(block_path,
                                     headers=upload_headers,
                                     body=upload_data)
        self.assertEqual(self.srmock.status, falcon.HTTP_405)
        self.assertTrue(self.srmock.headers_dict['x-block-location'])

        self.assertIn('x-storage-id', self.srmock.headers_dict)
        self.assertEqual(storage_block_id,
                         self.srmock.headers_dict['x-storage-id'])
        self.assertIn('x-block-id', self.srmock.headers_dict)
        self.assertEqual(upload_block_id,
                         self.srmock.headers_dict['x-block-id'])

    def test_put_block_vault_name_is_storage(self):
        # Rebuild the vault data
        self.vault_name = 'storage'
        self._vault_path = '/v1.0/vaults/{0}'.format(self.vault_name)
        self._blocks_path = '{0}/blocks'.format(self._vault_path)
        self._storage_path = '{0:}/storage'.format(self._vault_path)
        self._block_storage_path = '{0:}/blocks'.format(self._storage_path)
        self.helper_create_vault(self.vault_name, self._hdrs)

        block_id = self.create_storage_block_id()

        block_path = 'http://localhost{0}/{1}'.format(self._blocks_path,
                                                      block_id)
        storage_block_path = self.get_storage_block_path(block_id)

        response = self.simulate_put(storage_block_path,
                                     headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_405)
        self.assertIn('x-storage-id', self.srmock.headers_dict)
        self.assertEqual(block_id, self.srmock.headers_dict['x-storage-id'])
        self.assertIn('x-block-id', self.srmock.headers_dict)
        self.assertIsNone(self.srmock.headers_dict['x-block-id'])

        block_location = self.srmock.headers_dict['x-block-location']
        self.assertTrue(block_location)
        self.assertIn('storage', block_location)
        self.assertEqual(block_path, block_location)

    def test_list_blocks_bad_vault(self):
        vault_path = '/v1.0/vaults/{0}'.format(self.create_vault_id())
        storage_path = '{0:}/storage'.format(vault_path)
        block_storage_path = '{0:}/blocks'.format(storage_path)
        response = self.simulate_get(block_storage_path,
                                     headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_404)

    def test_list_blocks(self):
        response = self.simulate_get(self._block_storage_path,
                                     headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_501)

    def test_list_blocks_with_marker(self):
        block_marker = self.create_storage_block_id()
        marker = 'marker={0:}'.format(block_marker)
        response = self.simulate_get(self._block_storage_path,
                                     query_string=marker,
                                     headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_501)

    def test_head_block(self):
        block_id = self.create_storage_block_id()

        block_path = self.get_storage_block_path(block_id)

        response = self.simulate_head(block_path,
                                      headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_501)

    def test_get_block(self):
        block_id = self.create_storage_block_id()

        block_path = self.get_storage_block_path(block_id)

        response = self.simulate_get(block_path,
                                     headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_501)

    def test_delete_block(self):
        block_id = self.create_storage_block_id()

        block_path = self.get_storage_block_path(block_id)

        response = self.simulate_delete(block_path,
                                        headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_501)
