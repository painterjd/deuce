import ddt
import hashlib
import falcon
import os
from mock import patch
from deuce.model import Block

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

    def test_put_block_nonexistant_block(self):
        # No block already in metadata/storage

        block_id = self.create_storage_block_id()

        block_path = self.get_storage_block_path(self.vault_name, block_id)

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
        upload_block_path = self.get_block_path(self.vault_name,
                                                upload_block_id)
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
            self.vault_name, storage_block_id)

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
        storage_block_path = self.get_storage_block_path(
            self.vault_name, block_id)

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
        block_marker = self.create_storage_block_id()
        marker = 'marker={0:}'.format(block_marker)
        response = self.simulate_get(self._block_storage_path,
                                     query_string=marker,
                                     headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_501)

    def test_head_block_in_nonexistent_vault(self):
        block_id = self.create_storage_block_id()

        block_path = self.get_storage_block_path(self.create_vault_id(),
                                                 block_id)

        response = self.simulate_head(block_path,
                                      headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_404)

    def test_head_block_nonexistent_block(self):
        block_id = self.create_storage_block_id()

        block_path = self.get_storage_block_path(self.vault_name, block_id)

        response = self.simulate_head(block_path,
                                      headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_404)

    def test_head_orphaned_block(self):
        block_list, block_data = self.helper_create_blocks(num_blocks=1)
        self.assertEqual(len(block_list), 1)

        size, data, sha1 = zip(*block_data)

        block_path = self.get_block_path(self.vault_name, sha1[0])

        upload_headers = {}
        upload_headers.update(self._hdrs)
        upload_headers.update({
            "Content-Type": "application/octet-stream",
            "Content-Length": str(size[0]),
        })

        # Upload the block twice, orphaning the second block
        upload_first = self.simulate_put(block_path,
                                         headers=upload_headers,
                                         body=data[0])
        self.assertEqual(self.srmock.status, falcon.HTTP_201)
        first_storage_id = self.srmock.headers_dict['x-storage-id']
        first_block_id = self.srmock.headers_dict['x-block-id']

        upload_second = self.simulate_put(block_path,
                                          headers=upload_headers,
                                         body=data[0])
        self.assertEqual(self.srmock.status, falcon.HTTP_201)
        second_storage_id = self.srmock.headers_dict['x-storage-id']
        second_block_id = self.srmock.headers_dict['x-block-id']

        # Verify we got the same block id but different storage ids
        self.assertEqual(first_block_id, second_block_id)
        self.assertNotEqual(first_storage_id, second_storage_id)

        # Get the storage id from the orphaned second block
        storage_id = second_storage_id

        storage_block_path = self.get_storage_block_path(self.vault_name,
                                                         storage_id)
        # Now try to head the orphaned block
        response = self.simulate_head(storage_block_path,
                                      headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_204)

        self.assertIn('x-block-size', self.srmock.headers_dict)
        self.assertEqual(str(size[0]),
                         self.srmock.headers_dict['x-block-size'])

        self.assertIn('x-storage-id', self.srmock.headers_dict)
        self.assertEqual(storage_id, self.srmock.headers_dict['x-storage-id'])

        self.assertIn('x-block-id', self.srmock.headers_dict)
        self.assertEqual('None', self.srmock.headers_dict['x-block-id'])

        self.assertIn('x-ref-modified', self.srmock.headers_dict)
        self.assertEqual('None', self.srmock.headers_dict['x-ref-modified'])

        self.assertIn('x-block-reference-count', self.srmock.headers_dict)
        self.assertEqual('0',
                         self.srmock.headers_dict['x-block-reference-count'])

        self.assertIn('x-block-orphaned', self.srmock.headers_dict)
        self.assertEqual(str(True),
                         self.srmock.headers_dict['x-block-orphaned'])

    def test_head_happy_path(self):
        block_list, block_data = self.helper_create_blocks(num_blocks=1)
        self.assertEqual(len(block_list), 1)

        size, data, sha1 = zip(*block_data)

        block_path = self.get_block_path(self.vault_name, sha1[0])

        upload_headers = {}
        upload_headers.update(self._hdrs)
        upload_headers.update({
            "Content-Type": "application/octet-stream",
            "Content-Length": str(size[0]),
        })

        upload = self.simulate_put(block_path,
                                   headers=upload_headers,
                                   body=data[0])
        self.assertEqual(self.srmock.status, falcon.HTTP_201)

        # Get the storage id from the orphaned second block
        storage_id = self.srmock.headers_dict['x-storage-id']

        storage_block_path = self.get_storage_block_path(self.vault_name,
                                                         storage_id)
        # Now try to head the block
        response = self.simulate_head(storage_block_path,
                                      headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_204)

        self.assertIn('x-storage-id', self.srmock.headers_dict)
        self.assertEqual(storage_id, self.srmock.headers_dict['x-storage-id'])

        self.assertIn('x-block-id', self.srmock.headers_dict)
        self.assertEqual(sha1[0], self.srmock.headers_dict['x-block-id'])

        self.assertIn('x-ref-modified', self.srmock.headers_dict)
        self.assertNotEqual('None', self.srmock.headers_dict['x-ref-modified'])

        self.assertIn('x-block-reference-count', self.srmock.headers_dict)
        self.assertEqual('0',
                         self.srmock.headers_dict['x-block-reference-count'])

        self.assertIn('x-block-size', self.srmock.headers_dict)
        self.assertEqual(str(size[0]),
                         self.srmock.headers_dict['x-block-size'])

        self.assertIn('x-block-orphaned', self.srmock.headers_dict)
        self.assertEqual(str(False),
                         self.srmock.headers_dict['x-block-orphaned'])

    def test_get_block_invalid_block(self):
        block_id = self.create_storage_block_id()

        block_path = self.get_storage_block_path(self.vault_name, block_id)

        response = self.simulate_get(block_path, headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_404)

    def test_get_block_bad_vault(self):
        storage_block_id = self.create_storage_block_id()
        block_path = self.get_storage_block_path(self.create_vault_id(),
                                                 storage_block_id)
        response = self.simulate_get(block_path, headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_404)

    def test_get_block(self):
        block_list, block_data = self.helper_create_blocks(num_blocks=1)
        self.assertEqual(len(block_list), 1)

        storage_list = self.helper_store_blocks(self.vault_name, block_data)
        self.assertEqual(len(storage_list), 1)

        block_path = self.get_storage_block_path(
            self.vault_name, storage_list[0][1])

        response = self.simulate_get(block_path, headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_200)
        self.assertIn('x-ref-modified', self.srmock.headers_dict)
        self.assertIn('x-block-reference-count', self.srmock.headers_dict)
        self.assertEqual(
            int(self.srmock.headers_dict['x-block-reference-count']),
            0)
        self.assertIn('x-storage-id', self.srmock.headers_dict)
        self.assertEqual(storage_list[0][1],
                         self.srmock.headers_dict['x-storage-id'])
        self.assertIn('x-block-id', self.srmock.headers_dict)
        self.assertEqual(block_list[0],
                         self.srmock.headers_dict['x-block-id'])

        response_body = [resp for resp in response]
        bindata = response_body[0]
        z = hashlib.sha1()
        z.update(bindata)
        self.assertEqual(z.hexdigest(), block_list[0])

    def test_delete_storage_non_existent(self):
        storage_block_id = self.create_storage_block_id()

        storage_block_path = self.get_storage_block_path(self.vault_name,
                                                         storage_block_id)

        response = self.simulate_delete(storage_block_path,
                                        headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_404)

    def test_delete_storage_block_with_references(self):
        # NOTE(TheSriram): Let's just spoof ref-count to get 42 References.
        with patch.object(Block, 'get_ref_count', return_value=42):
            block_id = self.create_block_id(b'mock')
            response = self.simulate_put(self.get_block_path(self.vault_name,
                                                             block_id),
                                         headers=self._hdrs,
                                         body=b'mock')
            storage_block_id = self.srmock.headers_dict['x-storage-id']

            storage_block_path = self.get_storage_block_path(self.vault_name,
                                                             storage_block_id)

            response = self.simulate_delete(storage_block_path,
                                            headers=self._hdrs)
            self.assertEqual(self.srmock.status, falcon.HTTP_409)

    def test_delete_storage_orphaned_block(self):
        block_id = self.create_block_id(b'mock')
        # NOTE(TheSriram): We put the same block twice, to orphan the second
        # block as it will not have a reference in the metadata. But, it will
        # nevertheless be present in block storage
        response = self.simulate_put(self.get_block_path(self.vault_name,
                                                         block_id),
                                     headers=self._hdrs,
                                     body=b'mock')
        real_storage_id = self.srmock.headers_dict['x-storage-id']
        response = self.simulate_put(self.get_block_path(self.vault_name,
                                                         block_id),
                                     headers=self._hdrs,
                                     body=b'mock')
        orphaned_storage_id = self.srmock.headers_dict['x-storage-id']

        self.assertNotEqual(real_storage_id, orphaned_storage_id)

        storage_block_path = self.get_storage_block_path(self.vault_name,
                                                         orphaned_storage_id)

        response = self.simulate_delete(storage_block_path,
                                        headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_204)
