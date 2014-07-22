from unittest import TestCase
from webtest import TestApp
from deuce.tests import FunctionalTest

from deuce.model import Vault, Block, File


class TestModel(FunctionalTest):

    def setUp(self):
        super(TestModel, self).setUp()

        self.project_id = 'test_project_id'
        self.auth_token = 'test_auth_token'
        self.storage_url = 'test_storage_url'

    def test_get_nonexistent_block(self):
        v = Vault.get(self.project_id, 'should_not_exist',
                self.auth_token, self.storage_url)
        assert v is None

    def test_vault_crud(self):
        vault_id = 'my_vault_id_1'

        v = Vault.get(self.project_id, vault_id,
                self.auth_token, self.storage_url)
        assert v is None

        v = Vault.create(self.project_id, vault_id,
                self.auth_token, self.storage_url)
        assert v is not None

        v.delete(self.auth_token, self.storage_url)

        v = Vault.get(self.project_id, vault_id,
                self.auth_token, self.storage_url)
        assert v is None

    def test_file_crud(self):
        vault_id = 'my_vault_id_2'

        v = Vault.create(self.project_id, vault_id,
                self.auth_token, self.storage_url)

        f = v.create_file()

        assert isinstance(f, File)
        assert f.vault_id == vault_id

        file_id = f.file_id

        assert(len(file_id) > 0)

        file2 = v.get_file(file_id)

        assert isinstance(file2, File)
        assert file2.file_id == file_id
        assert file2.project_id == self.project_id

    def test_block_crud(self):
        vault_id = 'block_test_vault'

        v = Vault.create(self.project_id, vault_id,
                self.auth_token, self.storage_url)

        # Check for blocks, should be none
        blocks_gen = v.get_blocks(0, 0)
        blocks_list = list(blocks_gen)

        assert len(blocks_list) == 0
