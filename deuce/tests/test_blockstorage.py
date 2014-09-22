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
        self._storage_path = '{0:}/storage'.format(self._vault_path)
        self._block_storage_path = '{0:}/blocks'.format(self._storage_path)

        self._hdrs = {"x-project-id": self.create_project_id()}
        self.helper_create_vault(self.vault_name, self._hdrs)

    def tearDown(self):
        self.helper_delete_vault(self.vault_name, self._hdrs)
        super(TestBlockStorageController, self).tearDown()

    def get_storage_block_path(self, block_id):
        return '{0:}/{1:}'.format(self._block_storage_path, block_id)

    def test_put_block(self):
        block_id = self.create_block_id()

        block_path = self.get_storage_block_path(block_id)

        response = self.simulate_put(block_path,
                                     headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_405)

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
        block_marker = self.create_block_id()
        marker = 'marker={0:}'.format(block_marker)
        response = self.simulate_get(self._block_storage_path,
                                     query_string=marker,
                                     headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_501)

    def test_head_block(self):
        block_id = self.create_block_id()

        block_path = self.get_storage_block_path(block_id)

        response = self.simulate_head(block_path,
                                      headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_501)

    def test_get_block(self):
        block_id = self.create_block_id()

        block_path = self.get_storage_block_path(block_id)

        response = self.simulate_get(block_path,
                                     headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_501)

    def test_delete_block(self):
        block_id = self.create_block_id()

        block_path = self.get_storage_block_path(block_id)

        response = self.simulate_delete(block_path,
                                        headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_501)
