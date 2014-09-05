from tests.api import base

import ddt
import msgpack
import os
import sha


class TestNoBlocksUploaded(base.TestBase):

    def setUp(self):
        super(TestNoBlocksUploaded, self).setUp()
        self.create_empty_vault()

    def test_list_blocks_empty_vault(self):
        """List blocks for an empty vault"""

        resp = self.client.list_of_blocks(self.vaultname)
        self.assertEqual(resp.status_code, 200,
                         'Status code for listing all blocks is'
                         ' {0} . Expected 200'.format(resp.status_code))
        self.assertHeaders(resp.headers, json=True)
        self.assertListEqual(resp.json(), [],
                             'Response to List Blocks for an empty vault '
                             'should be an empty list []')

    def test_get_missing_block(self):
        """Get a block that has not been uploaded"""

        resp = self.client.get_block(self.vaultname, self.id_generator(50))
        self.assertEqual(resp.status_code, 404,
                         'Status code returned: {0} . '
                         'Expected 404'.format(resp.status_code))
        self.assertHeaders(resp.headers)

    def tearDown(self):
        super(TestNoBlocksUploaded, self).tearDown()
        self.client.delete_vault(self.vaultname)


@ddt.ddt
class TestUploadBlocks(base.TestBase):

    def setUp(self):
        super(TestUploadBlocks, self).setUp()
        self.create_empty_vault()

    @ddt.data(1, 100, 10000, 30720, 61440)
    def test_upload_block(self, value):
        """Upload a block to a vault"""

        self.generate_block_data(size=value)
        resp = self.client.upload_block(self.vaultname, self.blockid,
                                        self.block_data)
        self.assertEqual(resp.status_code, 201,
                         'Status code for uploading a block is '
                         '{0} . Expected 201'.format(resp.status_code))
        self.assertHeaders(resp.headers)
        self.assertEqual(len(resp.content), 0,
                         'Response Content was not empty. Content: '
                         '{0}'.format(resp.content))

    @ddt.data(1, 3, 10, 32)
    def test_upload_multiple_blocks(self, value):
        """Upload multiple blocks in a single request"""

        [self.generate_block_data() for _ in range(value)]
        data = dict([(block.Id, block.Data) for block in self.blocks])
        msgpacked_data = msgpack.packb(data)
        resp = self.client.upload_multiple_blocks(self.vaultname,
                                                  msgpacked_data)
        self.assertEqual(resp.status_code, 201,
                         'Status code for uploading multiple blocks is '
                         '{0} . Expected 201'.format(resp.status_code))
        self.assertHeaders(resp.headers)
        self.assertEqual(len(resp.content), 0,
                         'Response Content was not empty. Content: '
                         '{0}'.format(resp.content))

    def tearDown(self):
        super(TestUploadBlocks, self).tearDown()
        [self.client.delete_block(self.vaultname, block.Id) for block in
            self.blocks]
        self.client.delete_vault(self.vaultname)


class TestBlockUploaded(base.TestBase):

    def setUp(self):
        super(TestBlockUploaded, self).setUp()
        self.create_empty_vault()
        self.upload_block()

    def test_list_one_block(self):
        """List a single block"""

        resp = self.client.list_of_blocks(self.vaultname)
        self.assertEqual(resp.status_code, 200,
                         'Status code for listing all blocks is '
                         '{0} . Expected 200'.format(resp.status_code))
        self.assertHeaders(resp.headers, json=True)
        self.assertListEqual(resp.json(), [self.blockid],
                             'Response for List Blocks should have 1 item')

    def test_get_one_block(self):
        """Get an individual block"""

        resp = self.client.get_block(self.vaultname, self.blockid)
        self.assertEqual(resp.status_code, 200,
                         'Status code for getting data of a block is '
                         '{0} . Expected 200'.format(resp.status_code))
        self.assertHeaders(resp.headers, binary=True)
        self.assertIn('X-Block-Reference-Count', resp.headers)
        self.assertEqual(resp.headers['X-Block-Reference-Count'], '0')
        self.assertEqual(resp.content, self.block_data,
                         'Block data returned does not match block uploaded')

    def test_delete_block(self):
        """Delete one block"""

        resp = self.client.delete_block(self.vaultname, self.blockid)
        self.assertEqual(resp.status_code, 204,
                         'Status code for deleting a block is '
                         '{0} . Expected 204'.format(resp.status_code))
        self.assertHeaders(resp.headers)
        self.assertEqual(len(resp.content), 0,
                         'Response Content was not empty. Content: '
                         '{0}'.format(resp.content))

    def tearDown(self):
        super(TestBlockUploaded, self).tearDown()
        self.client.delete_block(self.vaultname, self.blockid)
        self.client.delete_vault(self.vaultname)


@ddt.ddt
class TestListBlocks(base.TestBase):

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
        self.assertEqual(resp.status_code, 200,
                         'Status code for listing all blocks is '
                         '{0} . Expected 200'.format(resp.status_code))
        self.assertHeaders(resp.headers, json=True)
        self.assertListEqual(sorted(resp.json()), sorted(self.blockids),
                             'Response for List Blocks'
                             ' {0} {1}'.format(self.blockids, resp.json()))

    @ddt.data(2, 4, 5, 10)
    def test_list_multiple_blocks_marker(self, value):
        """List multiple blocks (20) using a marker (value)"""

        sorted_block_list = sorted(self.blockids)
        markerid = sorted_block_list[value]
        resp = self.client.list_of_blocks(self.vaultname, marker=markerid)
        self.assertEqual(resp.status_code, 200,
                         'Status code for listing all blocks is '
                         '{0} . Expected 200'.format(resp.status_code))
        self.assertHeaders(resp.headers, json=True)
        self.assertListEqual(sorted(resp.json()), sorted_block_list[value:],
                             'Response for List Blocks'
                             ' {0} {1}'.format(self.blockids, resp.json()))

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

            self.assertEqual(resp.status_code, 200,
                             'Status code for listing all blocks is '
                             '{0} . Expected 200'.format(resp.status_code))
            self.assertHeaders(resp.headers, json=True)
            if i < 20 / value - (1 + pages):
                self.assertIn('x-next-batch', resp.headers)
                url = resp.headers['x-next-batch']
                self.assertUrl(url, blocks=True, nextlist=True)
            else:
                self.assertNotIn('x-next-batch', resp.headers)
            self.assertEqual(len(resp.json()), value,
                             'Number of block ids returned is not {0} . '
                             'Returned {1}'.format(value, len(resp.json())))
            for blockid in resp.json():
                self.assertIn(blockid, self.blockids)
                self.blockids.remove(blockid)
        self.assertEqual(len(self.blockids), value * pages,
                         'Discrepancy between the list of blocks returned '
                         'and the blocks uploaded')

    def tearDown(self):
        super(TestListBlocks, self).tearDown()
        [self.client.delete_block(self.vaultname, block.Id) for block in
            self.blocks]
        self.client.delete_vault(self.vaultname)


class TestBlocksAssignedToFile(base.TestBase):

    def setUp(self):
        super(TestBlocksAssignedToFile, self).setUp()
        self.create_empty_vault()
        self.upload_multiple_blocks(3)
        self.create_new_file()
        self.assign_all_blocks_to_file()

    def test_delete_assigned_block(self):
        """Delete one block assigned to a file"""

        resp = self.client.delete_block(self.vaultname, self.blockid)
        self.assertEqual(resp.status_code, 412,
                         'Status code returned: {0} . '
                         'Expected 412'.format(resp.status_code))
        self.assertEqual(len(resp.content), 0,
                         'Response Content was not empty. Content: '
                         '{0}'.format(resp.content))

    def tearDown(self):
        super(TestBlocksAssignedToFile, self).tearDown()
        [self.client.delete_file(vaultname=self.vaultname,
            fileid=file_info.Id) for file_info in self.files]
        [self.client.delete_block(self.vaultname, block.Id) for block in
            self.blocks]
        self.client.delete_vault(self.vaultname)


@ddt.ddt
class TestBlocksReferenceCount(base.TestBase):

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

        expected = '3'
        if value == 'delete_finalized':
            # delete 1 reference; a finalized file
            self.client.delete_file(vaultname=self.vaultname,
                                    fileid=self.files[2].Id)
        elif value == 'delete_non_finalized':
            # delete 1 reference; a non-finalized file
            self.client.delete_file(vaultname=self.vaultname,
                                    fileid=self.files[0].Id)
        elif value == 'all':
            expected = '4'

        resp = self.client.get_block(self.vaultname, self.blockid)
        self.assertEqual(resp.status_code, 200,
                         'Status code for getting data of a block is '
                         '{0} . Expected 200'.format(resp.status_code))
        self.assertHeaders(resp.headers, binary=True)
        self.assertIn('X-Block-Reference-Count', resp.headers)
        self.assertEqual(resp.headers['X-Block-Reference-Count'], expected)
        self.assertEqual(resp.content, self.block_data,
                         'Block data returned does not match block uploaded')

    def tearDown(self):
        super(TestBlocksReferenceCount, self).tearDown()
        [self.client.delete_file(vaultname=self.vaultname,
            fileid=file_info.Id) for file_info in self.files]
        [self.client.delete_block(self.vaultname, block.Id) for block in
            self.blocks]
        self.client.delete_vault(self.vaultname)
