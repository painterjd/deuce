from random import randrange
import hashlib
import os
import json

import falcon
import mock
from mock import patch
from deuce.tests import ControllerTest
from deuce.util.misc import relative_uri


class TestVaults(ControllerTest):

    def setUp(self):
        super(TestVaults, self).setUp()
        self._hdrs = {"x-project-id": self.create_project_id()}

    def tearDown(self):
        super(TestVaults, self).tearDown()

    def test_vault_leaf(self):
        hdrs = self._hdrs
        vault_path = 'http://localhost/v1.0/vaults/'

        # Create an empty root path in the storage.
        self.helper_create_vault('vault_0', hdrs)
        self.helper_delete_vault('vault_0', hdrs)

        response = self.simulate_get('/v1.0/vaults/',
                                     headers=hdrs)
        self.assertEqual(str(response[0].decode()), str('{}'))

        # Prepare several vaults in the storage.
        for cnt in range(5):
            self.helper_create_vault('vault_{0}'.format(cnt), hdrs)

        # No limit nor marker

        response = self.simulate_get('/v1.0/vaults/',
                                     headers=hdrs)

        self.assertEqual(json.loads(response[0].decode()),
                         {"vault_3": {"url": vault_path + "vault_3"},
                          "vault_4": {"url": vault_path + "vault_4"},
                          "vault_2": {"url": vault_path + "vault_2"},
                          "vault_1": {"url": vault_path + "vault_1"},
                          "vault_0": {"url": vault_path + "vault_0"}}
                         )

        response = self.simulate_get('/v1.0/vaults/',
                                     query_string='marker=vault_0',
                                     headers=hdrs)
        self.assertEqual(json.loads(response[0].decode()),
                         {"vault_4": {"url": vault_path + "vault_4"},
                          "vault_2": {"url": vault_path + "vault_2"},
                          "vault_3": {"url": vault_path + "vault_3"},
                          "vault_0": {"url": vault_path + "vault_0"},
                          "vault_1": {"url": vault_path + "vault_1"}}
                         )

        # Only limit

        response = self.simulate_get('/v1.0/vaults/',
                                     query_string='limit=99',
                                     headers=hdrs)

        self.assertEqual(json.loads(response[0].decode()),
                         {"vault_4": {"url": vault_path + "vault_4"},
                          "vault_2": {"url": vault_path + "vault_2"},
                          "vault_3": {"url": vault_path + "vault_3"},
                          "vault_0": {"url": vault_path + "vault_0"},
                          "vault_1": {"url": vault_path + "vault_1"}}
                         )

        response = self.simulate_get('/v1.0/vaults/',
                                     query_string='limit=1',
                                     headers=hdrs)
        self.assertEqual(json.loads(response[0].decode()),
                         {"vault_0": {"url": vault_path + "vault_0"}}
                         )

        next_url = self.srmock.headers_dict["X-Next-Batch"]

        uri, querystring = relative_uri(next_url)
        new_querystring = querystring.replace('limit=1', 'limit=99')
        response = self.simulate_get(uri,
                                     query_string=new_querystring,
                                     headers=hdrs)

        self.assertEqual(json.loads(response[0].decode()),
                         {"vault_4": {"url": vault_path + "vault_4"},
                          "vault_2": {"url": vault_path + "vault_2"},
                          "vault_3": {"url": vault_path + "vault_3"},
                          "vault_1": {"url": vault_path + "vault_1"}}
                         )

        response = self.simulate_get(uri,
                                     query_string=querystring,
                                     headers=hdrs)
        self.assertEqual(json.loads(response[0].decode()),
                         {"vault_1": {"url": vault_path + "vault_1"}}
                         )

        response = self.simulate_get('/v1.0/vaults/',
                                     query_string='marker=vault_not_exists'
                                                  '&limit=99',
                                     headers=hdrs)
        self.assertEqual(str(response[0].decode()), str('{}'))

        # Cleanup
        for cnt in range(5):
            self.helper_delete_vault('vault_{0}'.format(cnt), hdrs)

    def test_invalid_vault_id(self):
        vault_name = '@#$@#$@$'
        vault_path = '/v1.0/vaults/{0}'.format(vault_name)

        # regex validation.
        response = self.simulate_put(vault_path, headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_400)

        response = self.simulate_head(vault_path, headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_404)

        response = self.simulate_get('/v1.0/vaults',
                                     query_string='marker=*',
                                     headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_404)

    def test_vault_deletion(self):

        # 1. Delete non-existent vault
        vault_name = self.create_vault_id()
        vault_path = '/v1.0/vaults/{0}'.format(vault_name)
        response = self.simulate_delete(vault_path,
                                        headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_404)

        # 2. Create Vault and Delete it (Empty Vault)
        vault_name = self.create_vault_id()
        vault_path = '/v1.0/vaults/{0}'.format(vault_name)
        response = self.simulate_put(vault_path,
                                     headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_201)

        response = self.simulate_delete(vault_path,
                                        headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_204)

        # 3. Create Vault, Add a Block, and Delete It (Non-Empty Vault)
        vault_name = self.create_vault_id()
        vault_path = '/v1.0/vaults/{0}'.format(vault_name)
        response = self.simulate_put(vault_path,
                                     headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_201)

        # Build a dummy block
        block_data = os.urandom(randrange(1, 2000))
        block_hash = hashlib.sha1()
        block_hash.update(block_data)
        block_id = block_hash.hexdigest()
        block_path = '{0:}/blocks/{1:}'.format(vault_path, block_id)

        # Upload a dummy block
        headers = {}
        headers.update(self._hdrs)

        headers['content-type'] = 'application/binary'
        headers['content-length'] = str(len(block_data))

        response = self.simulate_put(block_path, headers=headers,
                                     body=block_data)
        self.assertEqual(self.srmock.status, falcon.HTTP_201)

        # Delete the vault
        response = self.simulate_delete(vault_path,
                                        headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_412)

        # Delete the dummy block

        response = self.simulate_delete(block_path, headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_204)

        # Delete the vault
        response = self.simulate_delete(vault_path,
                                        headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_204)

    def test_vault_crud(self):
        vault_name = self.create_vault_id()
        vault_path = '/v1.0/vaults/{0}'.format(vault_name)

        # If we try to head the vault before it exists, it should
        # return a 404
        response = self.simulate_head(vault_path,
                                      headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_404)

        # If we try to get the statistics on the vault before it
        # exists, it should return a 404
        response = self.simulate_get(vault_path,
                                     headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_404)

        # Now we create the vault, which should return a 201 (created)
        response = self.simulate_put(vault_path, headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_201)

        # Now verify the vault exists
        response = self.simulate_head(vault_path, headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_204)

        # Now get the statistics, what do we get?
        # Base statistics:
        #   metadata (file count = 0, file-block count = 0, blocks = 0)
        #   storage (size = 0,...)
        # For now, just enforce we get a 200
        response = self.simulate_get(vault_path, headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_200)

        # Now delete the vault (this should be OK since it
        # contains nothing in it.
        response = self.simulate_delete(vault_path, headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_204)

        # Now we should get a 404 when trying to head the vault
        response = self.simulate_head(vault_path, headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_404)

        # Try to delete again, this time it should be a 404
        response = self.simulate_delete(vault_path, headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_404)

        # Delete a non-empty vault.
        response = self.simulate_put(vault_path, headers=self._hdrs)
        # Add a real block to it.
        block_data = os.urandom(2000)  # Data size : 2000.
        sha1 = hashlib.sha1()
        sha1.update(block_data)
        blockid = sha1.hexdigest()
        block_path = '{0}/blocks/{1}'.format(vault_path, blockid)
        block_headers = {
            "Content-Type": "application/binary",
            "Content-Length": "2000",
        }
        block_headers.update(self._hdrs)
        response = self.simulate_put(block_path, headers=block_headers,
                                     body=block_data)

        # Delete should fail.
        response = self.simulate_delete(vault_path, headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_412)

    def test_vault_error(self):
        from deuce.model import Vault
        with patch.object(Vault, 'create', return_value=False):
            self.simulate_put('/v1.0/vaults/error_vault', headers=self._hdrs)
            self.assertEqual(self.srmock.status, falcon.HTTP_500)
