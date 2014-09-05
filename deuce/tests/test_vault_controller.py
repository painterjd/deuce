from unittest import TestCase
from webtest import TestApp
from deuce.tests import FunctionalTest
import os
import hashlib
import random
import string
from random import randrange

import json


class TestVaultController(FunctionalTest):

    def setUp(self):
        super(TestVaultController, self).setUp()
        self._hdrs = {"x-project-id": self.create_project_id(),
            "x-auth-token": self.create_auth_token()}

    def helper_create_vault(self, vault_name, hdrs):
        vault_path = '/v1.0/vaults/{0}'.format(vault_name)
        response = self.app.put(vault_path, headers=hdrs)

    def helper_delete_vault(self, vault_name, hdrs):
        vault_path = '/v1.0/vaults/{0}'.format(vault_name)
        response = self.app.delete(vault_path, headers=hdrs)

    def test_vault_leaf(self):
        hdrs = {"x-project-id": self.create_project_id(),
            "x-auth-token": self.create_auth_token()}
        params = dict()

        # Create an empty root path in the storage.
        self.helper_create_vault('vault_0', hdrs)
        self.helper_delete_vault('vault_0', hdrs)

        response = self.app.get('/v1.0/vaults/',
            params=params,
            headers=hdrs)
        assert str(response.text) == str('[]')

        # Prepare several vaults in the storage.
        for cnt in range(5):
            self.helper_create_vault('vault_{0}'.format(cnt), hdrs)

        # No limit nor marker
        response = self.app.get('/v1.0/vaults/',
            params=params,
            headers=hdrs)

        assert json.loads(response.text) == json.loads(
            str(
                u'{"vault_4": {"url": "http://localhost/v1.0/vaults/vault_4"},'
                u' "vault_2": {"url": "http://localhost/v1.0/vaults/vault_2"},'
                u' "vault_3": {"url": "http://localhost/v1.0/vaults/vault_3"},'
                u' "vault_0": {"url": "http://localhost/v1.0/vaults/vault_0"},'
                u' "vault_1": {"url": "http://localhost/v1.0/vaults/vault_1"}}'
            ))

        # Only marker
        params['marker'] = "vault_0"
        response = self.app.get('/v1.0/vaults/',
            params=params,
            headers=hdrs)
        assert json.loads(response.text) == json.loads(
            str(
                u'{"vault_4": {"url": "http://localhost/v1.0/vaults/vault_4"},'
                u' "vault_2": {"url": "http://localhost/v1.0/vaults/vault_2"},'
                u' "vault_3": {"url": "http://localhost/v1.0/vaults/vault_3"},'
                u' "vault_0": {"url": "http://localhost/v1.0/vaults/vault_0"},'
                u' "vault_1": {"url": "http://localhost/v1.0/vaults/vault_1"}}'
            ))

        # Only limit
        params = dict()
        params['limit'] = 99
        response = self.app.get('/v1.0/vaults/',
            params=params,
            headers=hdrs)

        assert json.loads(response.text) == json.loads(
            str(
                u'{"vault_4": {"url": "http://localhost/v1.0/vaults/vault_4"},'
                u' "vault_2": {"url": "http://localhost/v1.0/vaults/vault_2"},'
                u' "vault_3": {"url": "http://localhost/v1.0/vaults/vault_3"},'
                u' "vault_0": {"url": "http://localhost/v1.0/vaults/vault_0"},'
                u' "vault_1": {"url": "http://localhost/v1.0/vaults/vault_1"}}'
            ))

        params['limit'] = 1
        response = self.app.get('/v1.0/vaults/',
            params=params,
            headers=hdrs)
        assert json.loads(response.text) == json.loads(
            str(
                u'{"vault_0": {"url": "http://localhost/v1.0/vaults/vault_0"}}'
            ))

        next_url = response.headers["X-Next-Batch"]
        params['limit'] = 99  # More than what left in the list.
        response = self.app.get(next_url,
            params=params,
            headers=hdrs)
        assert json.loads(response.text) == json.loads(
            str(
                u'{"vault_4": {"url": "http://localhost/v1.0/vaults/vault_4"},'
                u' "vault_2": {"url": "http://localhost/v1.0/vaults/vault_2"},'
                u' "vault_3": {"url": "http://localhost/v1.0/vaults/vault_3"},'
                u' "vault_1": {"url": "http://localhost/v1.0/vaults/vault_1"}}'
            ))

        params['limit'] = 1
        response = self.app.get(next_url,
            params=params,
            headers=hdrs)
        assert json.loads(response.text) == json.loads(
            str(
                u'{"vault_1": {"url": "http://localhost/v1.0/vaults/vault_1"}}'
            ))

        params['marker'] = 'vault_not_exists'
        params['limit'] = 99
        response = self.app.get('/v1.0/vaults/',
            params=params,
            headers=hdrs,
            expect_errors=True)
        assert str(response.text) == str('[]')

        # Cleanup
        for cnt in range(5):
            self.helper_delete_vault('vault_{0}'.format(cnt), hdrs)

    def test_invalid_vault_id(self):
        vault_name = '@#$@#$@$'
        vault_path = '/v1.0/vaults/{0}'.format(vault_name)

        # regex validation.
        response = self.app.put(vault_path, headers=self._hdrs,
                expect_errors=True)
        self.assertEqual(response.status_int, 400)

        response = self.app.head(vault_path, headers=self._hdrs,
                expect_errors=True)
        self.assertEqual(response.status_int, 404)

    def test_vault_deletion(self):

        # 1. Delete non-existent vault
        vault_name = self.create_vault_id()
        vault_path = '/v1.0/vaults/{0}'.format(vault_name)
        response = self.app.delete(vault_path,
            headers=self._hdrs, expect_errors=True)
        self.assertEqual(response.status_code, 404)

        # 2. Create Vault and Delete it (Empty Vault)
        vault_name = self.create_vault_id()
        vault_path = '/v1.0/vaults/{0}'.format(vault_name)
        response = self.app.put(vault_path,
            headers=self._hdrs)
        self.assertEqual(response.status_code, 201)

        response = self.app.delete(vault_path,
            headers=self._hdrs)
        self.assertEqual(response.status_code, 204)

        # 3. Create Vault, Add a Block, and Delete It (Non-Empty Vault)
        vault_name = self.create_vault_id()
        vault_path = '/v1.0/vaults/{0}'.format(vault_name)
        response = self.app.put(vault_path,
            headers=self._hdrs)
        self.assertEqual(response.status_code, 201)

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

        response = self.app.put(block_path, headers=headers,
            params=block_data)
        self.assertEqual(response.status_code, 201)

        # Delete the vault
        response = self.app.delete(vault_path,
            headers=self._hdrs, expect_errors=True)
        self.assertEqual(response.status_code, 412)

        # Delete the dummy block
        # TODO:
        # response = self.app.delete(block_path, headers=self._hdrs)
        # self.assertEqual(response.status_code, 204)
        #
        # # Delete the vault
        # response = self.app.delete(vault_path,
        #    headers = self._hdrs)
        # self.assertEqual(response.status_code, 204)

    def test_vault_crud(self):
        vault_name = self.create_vault_id()
        vault_path = '/v1.0/vaults/{0}'.format(vault_name)

        # If we try to head the vault before it exists, it should
        # return a 404
        response = self.app.head(vault_path,
            headers=self._hdrs, expect_errors=True)

        assert response.status_code == 404

        # If we try to get the statistics on the vault before it
        # exists, it should return a 404
        response = self.app.get(vault_path,
            headers=self._hdrs, expect_errors=True)

        assert response.status_code == 404

        # Now we create the vault, which should return a 201 (created)
        response = self.app.put(vault_path, headers=self._hdrs)
        assert response.status_code == 201

        # Now verify the vault exists
        response = self.app.head(vault_path, headers=self._hdrs)

        assert response.status_code == 204

        # Now get the statistics, what do we get?
        # Base statistics:
        #   metadata (file count = 0, file-block count = 0, blocks = 0)
        #   storage (size = 0,...)
        # For now, just enforce we get a 200
        response = self.app.get(vault_path, headers=self._hdrs)

        assert response.status_code == 200

        # Now delete the vault (this should be OK since it
        # contains nothing in it.
        response = self.app.delete(vault_path, headers=self._hdrs)

        assert response.status_code == 204

        # Now we should get a 404 when trying to head the vault
        response = self.app.head(vault_path, headers=self._hdrs,
            expect_errors=True)

        assert response.status_code == 404

        # Try to delete again, this time it should be a 404
        response = self.app.delete(vault_path, headers=self._hdrs,
            expect_errors=True)

        assert response.status_code == 404

        # Delete a non-empty vault.
        response = self.app.put(vault_path, headers=self._hdrs)
        # # Add a real block to it.
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
        response = self.app.put(block_path, headers=block_headers,
                                params=block_data)

        # # Delete should fail.
        response = self.app.delete(vault_path, headers=self._hdrs,
            expect_errors=True)
        assert response.status_code == 412
