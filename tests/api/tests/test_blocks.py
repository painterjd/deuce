from tests.api import base
from tests.api.utils.schema import deuce_schema

import ddt
import jsonschema
import msgpack
import os
import sha
import time


class TestNoBlocksUploaded(base.TestBase):
    _multiprocess_can_split_ = True

    def setUp(self):
        super(TestNoBlocksUploaded, self).setUp()
        self.create_empty_vault()

    def test_list_blocks_empty_vault(self):
        """List blocks for an empty vault"""

        resp = self.client.list_of_blocks(self.vaultname)
        self.assert_200_response(resp)

        resp_body = resp.json()
        jsonschema.validate(resp_body, deuce_schema.block_list)

        self.assertListEqual(resp_body, [],
                             'Response to List Blocks for an empty vault '
                             'should be an empty list []')

    def test_get_missing_block(self):
        """Get a block that has not been uploaded"""

        self.block_data = os.urandom(100)
        self.blockid = sha.new(self.block_data).hexdigest()
        resp = self.client.get_block(self.vaultname, self.blockid)
        self.assert_404_response(resp)

    def test_head_missing_block(self):
        """Head a block that has not been uploaded"""

        self.block_data = os.urandom(100)
        self.blockid = sha.new(self.block_data).hexdigest()
        resp = self.client.block_head(self.vaultname, self.blockid)
        self.assert_404_response(resp, skip_contentlength=True)

    def test_delete_missing_block(self):
        """Delete one missing block"""

        self.generate_block_data()
        resp = self.client.delete_block(self.vaultname, self.blockid)
        self.assert_404_response(resp)

    def test_upload_wrong_blockid(self):
        """Upload a block with a wrong blockid"""

        self.generate_block_data()
        bad_blockid = sha.new('bad').hexdigest()
        resp = self.client.upload_block(self.vaultname, bad_blockid,
                                        self.block_data)
        self.assert_412_response(resp)

        resp_body = resp.json()
        jsonschema.validate(resp_body, deuce_schema.error)

        self.assertEqual(resp_body['title'], 'Precondition Failure')
        self.assertEqual(resp_body['description'], 'hash error')

    def test_upload_multiple_wrong_blockid(self):
        """Upload a block with a wrong blockid"""

        self.generate_block_data()
        bad_blockid = sha.new('bad').hexdigest()
        data = dict([(bad_blockid, self.block_data)])
        msgpacked_data = msgpack.packb(data)
        resp = self.client.upload_multiple_blocks(self.vaultname,
                                                  msgpacked_data)
        self.assert_412_response(resp)

        resp_body = resp.json()
        jsonschema.validate(resp_body, deuce_schema.error)

        self.assertEqual(resp_body['title'], 'Precondition Failure')
        self.assertEqual(resp_body['description'],
                         'hash error')

    def tearDown(self):
        super(TestNoBlocksUploaded, self).tearDown()
        self.client.delete_vault(self.vaultname)


@ddt.ddt
class TestUploadBlocks(base.TestBase):
    _multiprocess_can_split_ = True

    def setUp(self):
        super(TestUploadBlocks, self).setUp()
        self.create_empty_vault()

    @ddt.data(1, 100, 10000, 30720, 61440)
    def test_upload_block(self, value):
        """Upload a block to a vault"""

        self.generate_block_data(size=value)
        resp = self.client.upload_block(self.vaultname, self.blockid,
                                        self.block_data)
        self.assert_201_response(resp)

        self.assertHeaders(resp.headers, blockid=self.blockid,
                           lastmodified=True, refcount=0)
        self.assertIn('x-storage-id', resp.headers)
        ids = resp.headers['x-storage-id'].split('_')
        self.assertEqual(ids[0], self.blockid,
                'Storage Id {0} does not begin with the block id {1}'
                ''.format(resp.headers['x-storage-id'], self.blockid))
        self.assert_uuid5(ids[1])

    @ddt.data(1, 3, 10, 32)
    def test_upload_multiple_blocks(self, value):
        """Upload multiple blocks in a single request"""

        [self.generate_block_data() for _ in range(value)]
        data = dict([(block.Id, block.Data) for block in self.blocks])
        msgpacked_data = msgpack.packb(data)
        resp = self.client.upload_multiple_blocks(self.vaultname,
                                                  msgpacked_data)
        self.assert_201_response(resp)

    def tearDown(self):
        super(TestUploadBlocks, self).tearDown()
        [self.client.delete_block(self.vaultname, block.Id) for block in
            self.blocks]
        self.client.delete_vault(self.vaultname)


class TestBlockUploaded(base.TestBase):
    _multiprocess_can_split_ = True

    def setUp(self):
        super(TestBlockUploaded, self).setUp()
        self.create_empty_vault()
        self.upload_block()

    def test_list_one_block(self):
        """List a single block"""

        time.sleep(5)
        resp = self.client.list_of_blocks(self.vaultname)
        self.assert_200_response(resp)

        resp_body = resp.json()
        jsonschema.validate(resp_body, deuce_schema.block_list)

        self.assertListEqual(resp_body, [self.blockid],
                             'Response for List Blocks should have 1 item')

        resp = self.client.block_head(self.vaultname, self.blockid)
        self.assert_204_response(resp)
        self.assertHeaders(resp.headers,
                           lastmodified=True,
                           skip_contentlength=True,
                           contentlength=0,
                           refcount=0,
                           blockid=self.blockid,
                           storageid=self.storageid)
        self.assertEqual(int(resp.headers['x-ref-modified']), self.modified)

    def test_get_one_block(self):
        """Get an individual block"""

        time.sleep(5)
        resp = self.client.get_block(self.vaultname, self.blockid)
        self.assertEqual(resp.status_code, 200,
                         'Status code returned: {0} . '
                         'Expected 200'.format(resp.status_code))
        self.assertHeaders(resp.headers,
                           binary=True,
                           lastmodified=True,
                           contentlength=len(self.block_data),
                           refcount=0,
                           blockid=self.blockid,
                           storageid=self.storageid)
        self.assertEqual(resp.content, self.block_data,
                         'Block data returned does not match block uploaded')

        resp = self.client.block_head(self.vaultname, self.blockid)
        self.assert_204_response(resp)
        self.assertHeaders(resp.headers,
                           lastmodified=True,
                           skip_contentlength=True,
                           contentlength=0,
                           refcount=0,
                           blockid=self.blockid,
                           storageid=self.storageid)
        self.assertEqual(int(resp.headers['x-ref-modified']), self.modified)

    def test_head_one_block(self):
        """Head an individual block"""

        time.sleep(5)
        resp = self.client.block_head(self.vaultname, self.blockid)
        self.assert_204_response(resp)
        self.assertHeaders(resp.headers,
                           lastmodified=True,
                           skip_contentlength=True,
                           contentlength=0,
                           refcount=0,
                           blockid=self.blockid,
                           storageid=self.storageid)
        self.assertEqual(int(resp.headers['x-ref-modified']), self.modified)
        self.assertEqual(len(resp.content), 0)

    def test_delete_block(self):
        """Delete one block"""

        resp = self.client.delete_block(self.vaultname, self.blockid)
        self.assert_204_response(resp)

    def test_upload_block_twice(self):
        """Upload the same block twice"""

        time.sleep(5)
        resp = self.client.upload_block(self.vaultname, self.blockid,
                                        self.block_data)
        self.assert_201_response(resp)

        self.assertHeaders(resp.headers,
                           blockid=self.blockid)
        self.assertIn('X-Storage-Id', resp.headers)
        self.storageid_added = resp.headers['X-Storage-Id']
        self.assertNotEqual(self.storageid, resp.headers['X-Storage-Id'])

        resp = self.client.block_head(self.vaultname, self.blockid)
        self.assert_204_response(resp)
        self.assertHeaders(resp.headers,
                           lastmodified=True,
                           skip_contentlength=True,
                           contentlength=0,
                           refcount=0,
                           blockid=self.blockid,
                           storageid=self.storageid)
        self.assertEqual(int(resp.headers['x-ref-modified']), self.modified)

    def test_upload_block_twice_with_msgpack(self):
        """Upload the same block twice, the second time using msgpack"""

        time.sleep(5)
        data = {self.blockid: self.block_data}
        msgpack_data = msgpack.packb(data)
        resp = self.client.upload_multiple_blocks(self.vaultname, msgpack_data)
        self.assert_201_response(resp)

        resp = self.client.block_head(self.vaultname, self.blockid)
        self.assert_204_response(resp)
        self.assertHeaders(resp.headers,
                           lastmodified=True,
                           skip_contentlength=True,
                           contentlength=0,
                           refcount=0,
                           blockid=self.blockid,
                           storageid=self.storageid)
        self.assertEqual(int(resp.headers['x-ref-modified']), self.modified)

    def tearDown(self):
        super(TestBlockUploaded, self).tearDown()
        if hasattr(self, 'storageid_added'):
            self.client.delete_storage_block(self.vaultname,
                    self.storageid_added)
        self.client.delete_block(self.vaultname, self.blockid)
        self.client.delete_vault(self.vaultname)


@ddt.ddt
class TestListBlocks(base.TestBase):
    _multiprocess_can_split_ = True

    def setUp(self):
        super(TestListBlocks, self).setUp()
        self.create_empty_vault()
        self.upload_multiple_blocks(20)
        self.blockids = []
        for block in self.blocks:
            self.blockids.append(block.Id)

    def test_list_multiple_blocks(self):
        """List multiple blocks (20)"""

        resp = self.client.list_of_blocks(self.vaultname)
        self.assert_200_response(resp)

        resp_body = resp.json()
        jsonschema.validate(resp_body, deuce_schema.block_list)

        self.assertListEqual(sorted(resp_body), sorted(self.blockids),
                             'Response for List Blocks'
                             ' {0} {1}'.format(self.blockids, resp_body))

    @ddt.data(2, 4, 5, 10)
    def test_list_multiple_blocks_marker(self, value):
        """List multiple blocks (20) using a marker (value)"""

        sorted_block_list = sorted(self.blockids)
        markerid = sorted_block_list[value]
        resp = self.client.list_of_blocks(self.vaultname, marker=markerid)
        self.assert_200_response(resp)

        resp_body = resp.json()
        jsonschema.validate(resp_body, deuce_schema.block_list)

        self.assertListEqual(sorted(resp_body), sorted_block_list[value:],
                             'Response for List Blocks'
                             ' {0} {1}'.format(self.blockids, resp_body))

    @ddt.data(2, 4, 5, 10)
    def test_list_blocks_limit(self, value):
        """List multiple blocks, setting the limit to value"""

        self.assertBlocksPerPage(value)

    @ddt.data(2, 4, 5, 10)
    def test_list_blocks_limit_marker(self, value):
        """List multiple blocks, setting the limit to value and using a
        marker"""

        markerid = sorted(self.blockids)[value]
        self.assertBlocksPerPage(value, marker=markerid, pages=1)

    def assertBlocksPerPage(self, value, marker=None, pages=0):
        """
        Helper function to check the blocks returned per request
        Also verifies that the marker, if provided, is used
        """

        url = None
        for i in range(20 / value - pages):
            if not url:
                resp = self.client.list_of_blocks(self.vaultname,
                                                  marker=marker, limit=value)
            else:
                resp = self.client.list_of_blocks(alternate_url=url)

            self.assert_200_response(resp)

            if i < 20 / value - (1 + pages):
                self.assertIn('x-next-batch', resp.headers)
                url = resp.headers['x-next-batch']
                self.assertUrl(url, blocks=True, nextlist=True)
            else:
                self.assertNotIn('x-next-batch', resp.headers)

            resp_body = resp.json()
            jsonschema.validate(resp_body, deuce_schema.block_list)

            self.assertEqual(len(resp_body), value,
                             'Number of block ids returned is not {0} . '
                             'Returned {1}'.format(value, len(resp_body)))
            for blockid in resp_body:
                self.assertIn(blockid, self.blockids)
                self.blockids.remove(blockid)
        self.assertEqual(len(self.blockids), value * pages,
                         'Discrepancy between the list of blocks returned '
                         'and the blocks uploaded')

    def test_list_blocks_invalid_marker(self):
        """Request a Vault list with an invalid marker"""

        bad_marker = self.id_generator(50) + '#$@'
        resp = self.client.list_of_blocks(self.vaultname, marker=bad_marker)
        self.assert_404_response(resp)

    def test_list_blocks_bad_marker(self):
        """Request a Block List with a bad marker.
        The marker is correctly formatted, but does not exist"""

        bad_marker = sha.new(self.id_generator(50)).hexdigest()
        blockids = self.blockids[:]
        blockids.append(bad_marker)
        blockids.sort()
        i = blockids.index(bad_marker)

        resp = self.client.list_of_blocks(self.vaultname, marker=bad_marker)
        self.assert_200_response(resp)
        resp_body = resp.json()
        jsonschema.validate(resp_body, deuce_schema.block_list)
        self.assertEqual(resp_body, blockids[i + 1:])

    def tearDown(self):
        super(TestListBlocks, self).tearDown()
        [self.client.delete_block(self.vaultname, block.Id) for block in
            self.blocks]
        self.client.delete_vault(self.vaultname)


class TestBlocksAssignedToFile(base.TestBase):
    _multiprocess_can_split_ = True

    def setUp(self):
        super(TestBlocksAssignedToFile, self).setUp()
        self.create_empty_vault()
        self.upload_multiple_blocks(3)
        self.create_new_file()
        time.sleep(5)
        self.assign_all_blocks_to_file()

    def test_delete_assigned_block(self):
        """Delete one block assigned to a file"""

        resp = self.client.delete_block(self.vaultname, self.blockid)
        self.assert_409_response(resp)

        resp_body = resp.json()
        jsonschema.validate(resp_body, deuce_schema.error)

        self.assertEqual(resp_body['title'], 'Conflict')
        self.assertEqual(resp_body['description'],
                '["Constraint Error: Block {0} has references"]'
                ''.format(self.blockid))

    def test_modified_block_after_assignment(self):
        """Head a block and compare the ref-modified value after the
        block was assigned to a file"""

        resp = self.client.block_head(self.vaultname, self.blockid)
        self.assert_204_response(resp)
        self.assertHeaders(resp.headers,
                           lastmodified=True,
                           skip_contentlength=True,
                           contentlength=0,
                           refcount=1,
                           blockid=self.blockid,
                           storageid=self.storageid)
        self.assertGreater(int(resp.headers['x-ref-modified']),
                           self.modified)

    def test_modified_block_after_removing_assignment(self):
        """Head a block and compare the ref-modified value after the
        number of references to the block is reduced"""

        resp = self.client.block_head(self.vaultname, self.blockid)
        self.assert_204_response(resp)
        self.modified = int(resp.headers['x-ref-modified'])
        time.sleep(5)
        resp = self.client.delete_file(self.vaultname, self.fileid)

        # Test begins here
        resp = self.client.block_head(self.vaultname, self.blockid)
        self.assert_204_response(resp)
        self.assertHeaders(resp.headers,
                           lastmodified=True,
                           skip_contentlength=True,
                           contentlength=0,
                           refcount=0,
                           blockid=self.blockid,
                           storageid=self.storageid)
        self.assertGreater(int(resp.headers['x-ref-modified']),
                           self.modified)

    def tearDown(self):
        super(TestBlocksAssignedToFile, self).tearDown()
        [self.client.delete_file(vaultname=self.vaultname,
            fileid=file_info.Id) for file_info in self.files]
        [self.client.delete_block(self.vaultname, block.Id) for block in
            self.blocks]
        self.client.delete_vault(self.vaultname)


@ddt.ddt
class TestBlocksReferenceCount(base.TestBase):
    _multiprocess_can_split_ = True

    def setUp(self):
        super(TestBlocksReferenceCount, self).setUp()
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
    def test_get_block_with_multiple_references(self, value):
        """Get an individual block that has multiple references"""

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

        resp = self.client.get_block(self.vaultname, self.blockid)
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
    def test_head_block_with_multiple_references(self, value):
        """Head an individual block that has multiple references"""

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

        resp = self.client.block_head(self.vaultname, self.blockid)
        self.assert_204_response(resp)
        self.assertHeaders(resp.headers,
                           lastmodified=True,
                           skip_contentlength=True,
                           contentlength=0,
                           refcount=expected,
                           blockid=self.blockid,
                           storageid=self.storageid)
        self.assertEqual(len(resp.content), 0)

    def tearDown(self):
        super(TestBlocksReferenceCount, self).tearDown()
        [self.client.delete_file(vaultname=self.vaultname,
            fileid=file_info.Id) for file_info in self.files]
        [self.client.delete_block(self.vaultname, block.Id) for block in
            self.blocks]
        self.client.delete_vault(self.vaultname)


class TestAssignBlocksFirst(base.TestBase):
    _multiprocess_can_split_ = True

    def setUp(self):
        super(TestAssignBlocksFirst, self).setUp()
        self.create_empty_vault()
        self.create_new_file()
        [self.generate_block_data() for _ in range(3)]
        self.assign_all_blocks_to_file()

    def test_upload_assigned_block(self):
        """Upload a block that has already been assigned to 1 file"""

        resp = self.client.upload_block(self.vaultname,
                                        self.blocks[0].Id,
                                        self.blocks[0].Data)
        self.assert_201_response(resp)

        self.assertHeaders(resp.headers, blockid=self.blocks[0].Id,
                           lastmodified=True, refcount=1)
        self.assertIn('x-storage-id', resp.headers)
        ids = resp.headers['x-storage-id'].split('_')
        self.assertEqual(ids[0], self.blocks[0].Id,
                'Storage Id {0} does not begin with the block id {1}'
                ''.format(resp.headers['x-storage-id'], self.blocks[0].Id))
        self.assert_uuid5(ids[1])

    def test_upload_multiple_assigned_blocks(self):
        """Upload multiple blocks that have already been assigned to 1 file"""

        data = dict([(block.Id, block.Data) for block in self.blocks])
        msgpack_data = msgpack.packb(data)
        resp = self.client.upload_multiple_blocks(self.vaultname, msgpack_data)
        self.assert_201_response(resp)

        for block in self.blocks:
            resp = self.client.block_head(self.vaultname, block.Id)
            self.assert_204_response(resp)
            self.assertHeaders(resp.headers,
                               lastmodified=True,
                               skip_contentlength=True,
                               contentlength=0,
                               refcount=1,
                               blockid=block.Id)
            self.assertIn('x-storage-id', resp.headers)
            ids = resp.headers['x-storage-id'].split('_')
            self.assertEqual(ids[0], block.Id,
                    'Storage Id {0} does not begin with the block id {1}'
                    ''.format(resp.headers['x-storage-id'], block.Id))
            self.assert_uuid5(ids[1])

    def tearDown(self):
        super(TestAssignBlocksFirst, self).tearDown()
        [self.client.delete_file(vaultname=self.vaultname,
            fileid=file_info.Id) for file_info in self.files]
        [self.client.delete_block(self.vaultname, block.Id) for block in
            self.blocks]
        self.client.delete_vault(self.vaultname)
