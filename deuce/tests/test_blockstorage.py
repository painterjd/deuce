import ddt
import hashlib
import falcon

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

    def test_put_block(self):
        block_id = self.create_block_id()

        block_path = self.get_storage_block_path(self.vault_name, block_id)

        response = self.simulate_put(block_path,
                                     headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_405)

    def test_list_blocks_bad_vault(self):
        block_storage_path = self.get_storage_blocks_path(
            self.create_vault_id())
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

        block_path = self.get_storage_block_path(self.vault_name, block_id)

        response = self.simulate_head(block_path,
                                      headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_501)

    def test_get_block_no_block(self):
        block_id = self.create_block_id()

        block_path = self.get_storage_block_path(self.vault_name, block_id)

        response = self.simulate_get(block_path, headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_404)

    def test_get_block_bad_vault(self):
        block_path = self.get_storage_block_path(self.create_vault_id(),
                                                 self.create_block_id())
        response = self.simulate_get(block_path, headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_404)

    def test_get_block(self):
        block_list, block_data = self.helper_create_blocks(num_blocks=1)
        self.assertEqual(len(block_list), 1)

        self.helper_store_blocks(self.vault_name, block_data)

        block_path = self.get_storage_block_path(
            self.vault_name, block_list[0])

        response = self.simulate_get(block_path, headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_200)
        # TODO: self.assertIn('x-ref-modified', str(self.srmock.headers))
        self.assertIn('x-block-reference-count', str(self.srmock.headers))
        self.assertEqual(
            int(self.srmock.headers_dict['x-block-reference-count']),
            0)

        response_body = [resp for resp in response]
        bindata = response_body[0]
        z = hashlib.sha1()
        z.update(bindata)
        self.assertEqual(z.hexdigest(), block_list[0])

    def test_delete_block(self):
        block_id = self.create_block_id()

        block_path = self.get_storage_block_path(self.vault_name, block_id)

        response = self.simulate_delete(block_path, headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_501)
