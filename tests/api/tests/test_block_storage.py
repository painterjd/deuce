from tests.api import base
from tests.api.utils.schema import deuce_schema

import ddt
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

        blockid = sha.new(self.id_generator(15)).hexdigest()
        st_id = uuid.uuid5(uuid.NAMESPACE_URL, self.id_generator(50))
        storageid = '{0}_{1}'.format(blockid, st_id)
        resp = self.client.get_storage_block(self.vaultname, storageid)
        self.assert_404_response(resp)

    def test_head_missing_storage_block(self):
        """Head a block that has not been uploaded"""

        blockid = sha.new(self.id_generator(15)).hexdigest()
        st_id = uuid.uuid5(uuid.NAMESPACE_URL, self.id_generator(50))
        storageid = '{0}_{1}'.format(blockid, st_id)
        resp = self.client.storage_block_head(self.vaultname, storageid)
        self.assert_404_response(resp, skip_contentlength=True)

    def test_upload_missing_storage_block(self):
        """Try to upload a block to storage block.
        Block not present in metadata"""

        self.generate_block_data()
        blockid = sha.new(self.id_generator(15)).hexdigest()
        st_id = uuid.uuid5(uuid.NAMESPACE_URL, self.id_generator(50))
        storageid = '{0}_{1}'.format(blockid, st_id)
        resp = self.client.upload_storage_block(self.vaultname, storageid,
                                                self.block_data)
        self.assertEqual(resp.status_code, 405,
                         'Status code returned: {0} . '
                         'Expected 405'.format(resp.status_code))
        self.assertHeaders(resp.headers,
                           contentlength=0,
                           blockid='None',
                           storageid=str(storageid),
                           allow='HEAD, GET, DELETE',
                           location=True)

        self.assertUrl(resp.headers['x-block-location'], blockpath=True)
        blockid = resp.headers['x-block-location'].split('/')[-1]
        self.assertEqual(blockid, str(storageid))

    def test_post_missing_storage_block(self):
        """Try to post to storage block.
        Block not present in metadata"""

        self.generate_block_data()
        blockid = sha.new(self.id_generator(15)).hexdigest()
        st_id = uuid.uuid5(uuid.NAMESPACE_URL, self.id_generator(50))
        storageid = '{0}_{1}'.format(blockid, st_id)
        resp = self.client.post_storage_block(self.vaultname, storageid,
                                              self.block_data)
        self.assertEqual(resp.status_code, 405,
                         'Status code returned: {0} . '
                         'Expected 405'.format(resp.status_code))
        self.assertHeaders(resp.headers,
                           contentlength=0,
                           blockid='None',
                           storageid=str(storageid),
                           allow='GET',
                           location=True)

        self.assertUrl(resp.headers['x-block-location'], blockpath=True)
        blockid = resp.headers['x-block-location'].split('/')[-1]
        self.assertEqual(blockid, str(storageid))

    def test_delete_missing_storage_block(self):
        """Delete a storage block that has not been uploaded"""

        blockid = sha.new(self.id_generator(15)).hexdigest()
        st_id = uuid.uuid5(uuid.NAMESPACE_URL, self.id_generator(50))
        storageid = '{0}_{1}'.format(blockid, st_id)
        resp = self.client.delete_storage_block(self.vaultname, storageid)
        self.assert_404_response(resp)

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
                           contentlength=len(self.block_data),
                           refcount=0,
                           blockid=self.blockid,
                           storageid=self.storageid)
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
                           skip_contentlength=True,
                           contentlength=0,
                           refcount=0,
                           blockid=self.blockid,
                           storageid=self.storageid,
                           orphan='False',
                           size=len(self.block_data))
        self.assertEqual(len(resp.content), 0)

    def test_upload_storage_block(self):
        """Try to upload a block to storage block.
        Block is present in metadata"""

        resp = self.client.upload_storage_block(self.vaultname, self.storageid,
                                                self.block_data)
        self.assertEqual(resp.status_code, 405,
                         'Status code returned: {0} . '
                         'Expected 405'.format(resp.status_code))
        self.assertHeaders(resp.headers,
                           contentlength=0,
                           blockid=self.blockid,
                           storageid=self.storageid,
                           allow='HEAD, GET, DELETE',
                           location=True)

        self.assertUrl(resp.headers['x-block-location'], blockpath=True)
        blockid = resp.headers['x-block-location'].split('/')[-1]
        self.assertEqual(blockid, self.blockid)

    def test_post_storage_block(self):
        """Try to post to storage block.
        Block is present in metadata"""

        resp = self.client.post_storage_block(self.vaultname,
                                              self.block_data)
        self.assertEqual(resp.status_code, 405,
                         'Status code returned: {0} . '
                         'Expected 405'.format(resp.status_code))
        self.assertHeaders(resp.headers,
                           contentlength=0,
                           allow='GET')

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


class TestBlockOrphaned(base.TestBase):

    def setUp(self):
        super(TestBlockOrphaned, self).setUp()
        self.create_empty_vault()
        self.upload_block()
        # Second block uploaded is orphaned
        self.upload_block(self.block_data, len(self.block_data))

    def test_get_one_orphaned_storage_block(self):
        """Get an individual, orphaned block from storage.
        Block information not present in metadata"""

        resp = self.client.get_storage_block(self.vaultname, self.storageid)
        self.assertEqual(resp.status_code, 200,
                         'Status code returned: {0} . '
                         'Expected 200'.format(resp.status_code))
        self.assertHeaders(resp.headers, binary=True,
                           lastmodified=True,
                           contentlength=len(self.block_data),
                           refcount=0,
                           blockid='None',
                           storageid=self.storageid)
        self.assertEqual(resp.content, self.block_data,
                         'Block data returned does not match block uploaded')

    def test_head_one_orphaned_storage_block(self):
        """Head an individual, orphaned storage block.
        Block information not present in metadata"""

        resp = self.client.storage_block_head(self.vaultname, self.storageid)
        self.assertEqual(resp.status_code, 204,
                         'Status code returned: {0} . '
                         'Expected 204'.format(resp.status_code))
        self.assertHeaders(resp.headers,
                           skip_contentlength=True,
                           contentlength=0,
                           refcount=0,
                           blockid='None',
                           storageid=self.storageid,
                           orphan='True',
                           size=len(self.block_data))
        self.assertIn('X-Ref-Modified', resp.headers)
        self.assertEqual(resp.headers['X-Ref-Modified'], "None")
        self.assertEqual(len(resp.content), 0)

    def test_delete_storage_block_with_metadata(self):
        """Delete one orphaned block from storage.
        Block information not present in metadata"""

        resp = self.client.delete_storage_block(self.vaultname, self.storageid)
        self.assert_204_response(resp)

    def tearDown(self):
        super(TestBlockOrphaned, self).tearDown()
        self.client.delete_storage_block(self.vaultname, self.storageid)
        self.client.delete_block(self.vaultname, self.blockid)
        self.client.delete_vault(self.vaultname)


@ddt.ddt
class TestListStorageBlocks(base.TestBase):

    def setUp(self):
        super(TestListStorageBlocks, self).setUp()
        self.create_empty_vault()
        self.upload_multiple_blocks(20)
        self.storageids = []
        for storage in self.storage:
            self.storageids.append(storage.Id)

    def test_list_multiple_storage_blocks(self):
        """List multiple storage blocks (20)"""

        resp = self.client.list_of_storage_blocks(self.vaultname)
        self.assert_200_response(resp)

        resp_body = resp.json()
        jsonschema.validate(resp_body, deuce_schema.block_storage_list)

        self.assertListEqual(sorted(resp_body), sorted(self.storageids),
                             'Response for List Storage Blocks'
                             ' {0} {1}'.format(self.storageids, resp_body))

    @ddt.data(2, 4, 5, 10)
    def test_list_multiple_storage_blocks_marker(self, value):
        """List multiple storage blocks (20) using a marker (value)"""

        sorted_storage_list = sorted(self.storageids)
        markerid = sorted_storage_list[value]
        resp = self.client.list_of_storage_blocks(self.vaultname,
                marker=markerid)
        self.assert_200_response(resp)

        resp_body = resp.json()
        jsonschema.validate(resp_body, deuce_schema.block_storage_list)

        self.assertListEqual(sorted(resp_body), sorted_storage_list[value:],
                             'Response for List Storage Blocks'
                             ' {0} {1}'.format(self.storageids, resp_body))

    @ddt.data(2, 4, 5, 10)
    def test_list_storage_blocks_limit(self, value):
        """List multiple storage blocks, setting the limit to value"""

        self.assertBlocksPerPage(value)

    @ddt.data(2, 4, 5, 10)
    def test_list_storage_blocks_limit_marker(self, value):
        """List multiple storage blocks, setting the limit to value and using a
        marker"""

        markerid = sorted(self.storageids)[value]
        self.assertBlocksPerPage(value, marker=markerid, pages=1)

    def assertBlocksPerPage(self, value, marker=None, pages=0):
        """
        Helper function to check the blocks returned per request
        Also verifies that the marker, if provided, is used
        """

        url = None
        for i in range(20 / value - pages):
            if not url:
                resp = self.client.list_of_storage_blocks(self.vaultname,
                                                  marker=marker, limit=value)
            else:
                resp = self.client.list_of_storage_blocks(alternate_url=url)

            self.assert_200_response(resp)

            if i < 20 / value - (1 + pages):
                self.assertIn('x-next-batch', resp.headers)
                url = resp.headers['x-next-batch']
                self.assertUrl(url, storage=True, nextlist=True)
            else:
                self.assertNotIn('x-next-batch', resp.headers)

            resp_body = resp.json()
            jsonschema.validate(resp_body, deuce_schema.block_storage_list)

            self.assertEqual(len(resp_body), value,
                      'Number of storage block ids returned is not {0} . '
                      'Returned {1}'.format(value, len(resp_body)))
            for storageid in resp_body:
                self.assertIn(storageid, self.storageids)
                self.storageids.remove(storageid)
        self.assertEqual(len(self.storageids), value * pages,
                         'Discrepancy between the list of storage blocks '
                         'returned and the blocks uploaded')

    def test_list_storage_blocks_invalid_marker(self):
        """Request a Storage Block List with an invalid marker"""

        bad_marker = sha.new(self.id_generator(50)).hexdigest()
        resp = self.client.list_of_storage_blocks(self.vaultname,
                marker=bad_marker)
        self.assert_404_response(resp)

    def test_list_storage_blocks_bad_marker(self):
        """Request a Storage Block List with a bad marker"""

        blockid = sha.new(self.id_generator(15)).hexdigest()
        st_id = uuid.uuid5(uuid.NAMESPACE_URL, self.id_generator(50))
        bad_storageid = '{0}_{1}'.format(blockid, st_id)
        resp = self.client.list_of_storage_blocks(self.vaultname,
                marker=bad_storageid)
        self.assert_200_response(resp)

        resp_body = resp.json()
        self.assertEqual(resp_body, [])

    def tearDown(self):
        super(TestListStorageBlocks, self).tearDown()
        [self.client.delete_block(self.vaultname, block.Id) for block in
            self.blocks]
        self.client.delete_vault(self.vaultname)


@ddt.ddt
class TestStorageBlocksReferenceCount(base.TestBase):

    def setUp(self):
        super(TestStorageBlocksReferenceCount, self).setUp()
        self.create_empty_vault()
        self.upload_block()
        # (not finalized) create two files and assign block
        for _ in range(2):
            self.create_new_file()
            self.assign_all_blocks_to_file()
        # (finalized) create two files and assign block
        for _ in range(2):
            self.create_new_file()
            self.assign_all_blocks_to_file()
            self.finalize_file()

    @ddt.data('all', 'delete_finalized', 'delete_non_finalized')
    def test_get_storage_block_with_multiple_references(self, value):
        """Get an individual storage block that has multiple references"""

        expected = 3
        if value == 'delete_finalized':
            # delete 1 reference; a finalized file
            self.client.delete_file(vaultname=self.vaultname,
                                    fileid=self.files[2].Id)
        elif value == 'delete_non_finalized':
            # delete 1 reference; a non-finalized file
            self.client.delete_file(vaultname=self.vaultname,
                                    fileid=self.files[0].Id)
        elif value == 'all':
            expected = 4

        resp = self.client.get_storage_block(self.vaultname, self.storageid)
        self.assertEqual(resp.status_code, 200,
                         'Status code returned: {0} . '
                         'Expected 200'.format(resp.status_code))
        self.assertHeaders(resp.headers, binary=True,
                           lastmodified=True,
                           contentlength=len(self.block_data),
                           refcount=expected,
                           blockid=self.blockid,
                           storageid=self.storageid)
        self.assertEqual(resp.content, self.block_data,
                         'Block data returned does not match block uploaded')

    @ddt.data('all', 'delete_finalized', 'delete_non_finalized')
    def test_head_storage_block_with_multiple_references(self, value):
        """Head an individual storage block that has multiple references"""

        expected = 3
        if value == 'delete_finalized':
            # delete 1 reference; a finalized file
            self.client.delete_file(vaultname=self.vaultname,
                                    fileid=self.files[2].Id)
        elif value == 'delete_non_finalized':
            # delete 1 reference; a non-finalized file
            self.client.delete_file(vaultname=self.vaultname,
                                    fileid=self.files[0].Id)
        elif value == 'all':
            expected = 4

        resp = self.client.storage_block_head(self.vaultname, self.storageid)
        self.assert_204_response(resp)
        self.assertHeaders(resp.headers,
                           lastmodified=True,
                           skip_contentlength=True,
                           contentlength=0,
                           refcount=expected,
                           blockid=self.blockid,
                           storageid=self.storageid,
                           orphan='False',
                           size=len(self.block_data))
        self.assertEqual(len(resp.content), 0)

    def tearDown(self):
        super(TestStorageBlocksReferenceCount, self).tearDown()
        [self.client.delete_file(vaultname=self.vaultname,
            fileid=file_info.Id) for file_info in self.files]
        [self.client.delete_block(self.vaultname, block.Id) for block in
            self.blocks]
        self.client.delete_vault(self.vaultname)
