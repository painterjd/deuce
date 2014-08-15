from tests.api import base

import ddt
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
                         ' {0}'.format(resp.status_code))
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

        block_data = os.urandom(value)
        self.blockid = sha.new(block_data).hexdigest()
        resp = self.client.upload_block(self.vaultname, self.blockid,
                                        block_data)
        self.assertEqual(resp.status_code, 201,
                         'Status code for uploading a block is '
                         '{0}'.format(resp.status_code))
        self.assertHeaders(resp.headers)
        self.assertEqual(len(resp.content), 0,
                         'Response Content was not empty. Content: '
                         '{0}'.format(resp.content))

    def tearDown(self):
        super(TestUploadBlocks, self).tearDown()
        if hasattr(self, 'blockid'):
            self.client.delete_block(self.vaultname, self.blockid)
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
                         '{0}'.format(resp.status_code))
        self.assertHeaders(resp.headers, json=True)
        self.assertListEqual(resp.json(), [self.blockid],
                             'Response for List Blocks should have 1 item')

    def test_get_one_block(self):
        """Get an individual block"""

        resp = self.client.get_block(self.vaultname, self.blockid)
        self.assertEqual(resp.status_code, 200,
                         'Status code for getting data of a block is '
                         '{0}'.format(resp.status_code))
        self.assertHeaders(resp.headers, binary=True)
        self.assertEqual(resp.content, self.block_data,
                         'Block data returned does not match block uploaded')

    def test_delete_block(self):
        """Delete one block"""

        # TODO
        self.skipTest('Skipping. Functionality not implemented')
        resp = self.client.delete_block(self.vaultname, self.blockid)
        self.assertEqual(resp.status_code, 204,
                         'Status code for deleting a block is '
                         '{0}'.format(resp.status_code))
        self.assertEqual(len(resp.content), 0)

    def tearDown(self):
        super(TestBlockUploaded, self).tearDown()
        if hasattr(self, 'blockid'):
            self.client.delete_block(self.vaultname, self.blockid)
        self.client.delete_vault(self.vaultname)


@ddt.ddt
class TestListBlocks(base.TestBase):

    def setUp(self):
        super(TestListBlocks, self).setUp()
        self.create_empty_vault()
        [self.upload_block() for _ in range(20)]
        self.blockids = []
        for block in self.blocks:
            self.blockids.append(block.Id)

    def test_list_multiple_blocks(self):
        """List multiple blocks (20)"""

        resp = self.client.list_of_blocks(self.vaultname)
        self.assertEqual(resp.status_code, 200,
                         'Status code for listing all blocks is '
                         '{0}'.format(resp.status_code))
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
                         '{0}'.format(resp.status_code))
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
                             '{0}'.format(resp.status_code))
            self.assertHeaders(resp.headers, json=True)
            if i < 20 / value - (1 + pages):
                self.assertIn('x-next-batch', resp.headers)
                url = resp.headers['x-next-batch']
                self.assertUrl(url, blockpath=True, nextlist=True)
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
