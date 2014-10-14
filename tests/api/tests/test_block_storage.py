from tests.api import base
from tests.api.utils.schema import deuce_schema

import jsonschema
import os
import sha
import uuid


class TestNoBlocksUploaded(base.TestBase):

    def setUp(self):
        super(TestNoBlocksUploaded, self).setUp()
        self.create_empty_vault()

    def test_get_missing_storage_block(self):
        """Get a storage block that has not been uploaded"""

        storageid = uuid.uuid5(uuid.NAMESPACE_URL, self.id_generator(50))
        resp = self.client.get_storage_block(self.vaultname, storageid)
        self.assert_404_response(resp)

    def test_head_missing_storage_block(self):
        """Head a block that has not been uploaded"""

        storageid = uuid.uuid5(uuid.NAMESPACE_URL, self.id_generator(50))
        resp = self.client.storage_block_head(self.vaultname, storageid)
        self.assert_404_response(resp)

    def test_upload_missing_storage_block(self):
        """Try to upload a block to storage block.
        Block not present in metadata"""

        self.generate_block_data()
        storageid = uuid.uuid5(uuid.NAMESPACE_URL, self.id_generator(50))
        resp = self.client.upload_storage_block(self.vaultname, storageid,
                                                self.block_data)
        self.assertEqual(resp.status_code, 405,
                         'Status code returned: {0} . '
                         'Expected 405'.format(resp.status_code))
        self.assertHeaders(resp.headers, contentlength=0)

        self.assertIn('x-block-id', resp.headers)
        self.assertEqual(resp.headers['x-block-id'], 'None')
        self.assertIn('x-storage-id', resp.headers)
        self.assertEqual(resp.headers['x-storage-id'], str(storageid))
        self.assertIn('allow', resp.headers)
        self.assertEqual(resp.headers['allow'],
                'HEAD, GET, DELETE')
        self.assertIn('x-block-location', resp.headers)
        self.assertUrl(resp.headers['x-block-location'], blockpath=True)
        blockid = resp.headers['x-block-location'].split('/')[-1]
        self.assertEqual(blockid, str(storageid))

    def tearDown(self):
        super(TestNoBlocksUploaded, self).tearDown()
        self.client.delete_vault(self.vaultname)


class TestBlockUploaded(base.TestBase):

    def setUp(self):
        super(TestBlockUploaded, self).setUp()
        self.create_empty_vault()
        self.upload_block()

    def test_get_one_storage_block(self):
        """Get an individual block from storage"""

        resp = self.client.get_storage_block(self.vaultname, self.storageid)
        self.assertEqual(resp.status_code, 200,
                         'Status code returned: {0} . '
                         'Expected 200'.format(resp.status_code))
        self.assertHeaders(resp.headers, binary=True,
                           lastmodified=True,
                           contentlength=len(self.block_data))
        self.assertIn('X-Block-Reference-Count', resp.headers)
        self.assertEqual(resp.headers['X-Block-Reference-Count'], '0')
        self.assertIn('x-block-id', resp.headers)
        self.assertEqual(resp.headers['x-block-id'], self.blockid)
        self.assertIn('x-storage-id', resp.headers)
        self.assert_uuid5(resp.headers['x-storage-id'])
        self.assertEqual(resp.headers['x-storage-id'], self.storageid)
        self.assertEqual(resp.content, self.block_data,
                         'Block data returned does not match block uploaded')

    def test_head_one_storage_block(self):
        """Head an individual storage block"""

        resp = self.client.storage_block_head(self.vaultname, self.storageid)
        self.assertEqual(resp.status_code, 204,
                         'Status code returned: {0} . '
                         'Expected 204'.format(resp.status_code))
        self.assertHeaders(resp.headers,
                           lastmodified=True,
                           contentlength=0)
        self.assertIn('X-Block-Reference-Count', resp.headers)
        self.assertEqual(resp.headers['X-Block-Reference-Count'], '0')
        self.assertIn('x-block-id', resp.headers)
        self.assertEqual(resp.headers['x-block-id'], self.blockid)
        self.assertIn('x-storage-id', resp.headers)
        self.assert_uuid5(resp.headers['x-storage-id'])
        self.assertEqual(resp.headers['x-storage-id'], self.storageid)
        self.assertIn('x-block-orphaned', resp.headers)
        self.assertEqual(resp.headers['x-block-orphaned'], 'False')
        self.assertIn('x-block-size', resp.headers)
        self.assertEqual(int(resp.headers['x-block-size']),
                len(self.block_data))
        self.assertEqual(len(resp.content), 0)

    def test_upload_storage_block(self):
        """Try to upload a block to storage block.
        Block is present in metadata"""

        resp = self.client.upload_storage_block(self.vaultname, self.storageid,
                                                self.block_data)
        self.assertEqual(resp.status_code, 405,
                         'Status code returned: {0} . '
                         'Expected 405'.format(resp.status_code))
        self.assertHeaders(resp.headers, contentlength=0)

        self.assertIn('x-block-id', resp.headers)
        self.assertEqual(resp.headers['x-block-id'], self.blockid)
        self.assertIn('x-storage-id', resp.headers)
        self.assertEqual(resp.headers['x-storage-id'], self.storageid)
        self.assertIn('allow', resp.headers)
        self.assertEqual(resp.headers['allow'],
                'HEAD, GET, DELETE')
        self.assertIn('x-block-location', resp.headers)
        self.assertUrl(resp.headers['x-block-location'], blockpath=True)
        blockid = resp.headers['x-block-location'].split('/')[-1]
        self.assertEqual(blockid, self.blockid)

    def test_delete_storage_block_with_metadata(self):
        """Delete one block from storage.
        Block information is present in metadata"""

        resp = self.client.delete_storage_block(self.vaultname, self.storageid)
        self.assert_409_response(resp)

        resp_body = resp.json()
        jsonschema.validate(resp_body, deuce_schema.error)

        self.assertEqual(resp_body['title'], 'Conflict')
        self.assertEqual(resp_body['description'],
                'Storage ID: {0} has 0 reference(s) in metadata'
                ''.format(self.storageid))

    def tearDown(self):
        super(TestBlockUploaded, self).tearDown()
        self.client.delete_block(self.vaultname, self.blockid)
        self.client.delete_vault(self.vaultname)
