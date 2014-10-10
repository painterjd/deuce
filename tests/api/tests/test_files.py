from tests.api import base
from tests.api.utils.schema import deuce_schema

import ddt
import json
import jsonschema
import os
import sha
import uuid


class TestNoFilesCreated(base.TestBase):

    def setUp(self):
        super(TestNoFilesCreated, self).setUp()
        self.create_empty_vault()

    def test_get_missing_file(self):
        """Get a file that has not been created"""

        resp = self.client.get_file(self.vaultname, self.id_generator(50))
        self.assert_404_response(resp)

    def test_delete_missing_file(self):
        """Delete a missing file"""

        resp = self.client.delete_file(self.vaultname, self.id_generator(50))
        self.assert_404_response(resp)

    def tearDown(self):
        super(TestNoFilesCreated, self).tearDown()
        self.client.delete_vault(self.vaultname)


class TestCreateFile(base.TestBase):

    def setUp(self):
        super(TestCreateFile, self).setUp()
        self.create_empty_vault()

    def test_create_file(self):
        """Create a file"""

        resp = self.client.create_file(self.vaultname)
        self.assert_201_response(resp)

        self.assertIn('location', resp.headers)
        self.assertUrl(resp.headers['location'], filepath=True)

    def tearDown(self):
        super(TestCreateFile, self).tearDown()
        if hasattr(self, 'fileid'):
            self.client.delete_file(vaultname=self.vaultname,
                                    fileid=self.fileid)
        self.client.delete_vault(self.vaultname)


class TestFileBlockUploaded(base.TestBase):

    def setUp(self):
        super(TestFileBlockUploaded, self).setUp()
        self.create_empty_vault()
        self.upload_block()
        self.create_new_file()

    def test_assign_block_to_file(self):
        """Assign an uploaded block to a file"""

        block_list = list()
        block_info = self.blocks[0]
        block_list.append([block_info.Id, 0])

        resp = self.client.assign_to_file(json.dumps(block_list),
                                          alternate_url=self.fileurl)

        self.assert_200_response(resp)

        resp_body = resp.json()
        jsonschema.validate(resp_body, deuce_schema.block_list)

        self.assertListEqual(resp_body, [])

    def test_assign_missing_block_to_file(self):
        """Assign a missing block to a file"""

        block_data = os.urandom(30720)
        blockid = sha.new(block_data).hexdigest()
        block_list = list()
        block_list.append([blockid, 0])

        resp = self.client.assign_to_file(json.dumps(block_list),
                                          alternate_url=self.fileurl)

        self.assert_200_response(resp)

        resp_body = resp.json()
        jsonschema.validate(resp_body, deuce_schema.block_list)

        self.assertListEqual(resp_body, [blockid])

    def tearDown(self):
        super(TestFileBlockUploaded, self).tearDown()
        [self.client.delete_file(vaultname=self.vaultname,
            fileid=file_info.Id) for file_info in self.files]
        [self.client.delete_block(self.vaultname, block.Id) for block in
            self.blocks]
        self.client.delete_vault(self.vaultname)


class TestEmptyFile(base.TestBase):

    def setUp(self):
        super(TestEmptyFile, self).setUp()
        self.create_empty_vault()
        self.create_new_file()

    def test_finalize_empty_file(self):
        """Finalize an empty file"""

        resp = self.client.finalize_file(filesize=0,
                                         alternate_url=self.fileurl)
        self.assertEqual(resp.status_code, 200,
                         'Status code returned: {0} . '
                         'Expected 200'.format(resp.status_code))
        self.assertHeaders(resp.headers, contentlength=0)
        self.assertEqual(len(resp.content), 0)

    def test_list_empty_file(self):
        """Get list of files with only one file that is empty and not
        finalized"""

        resp = self.client.list_of_files(vaultname=self.vaultname)
        self.assert_200_response(resp)

        resp_body = resp.json()
        jsonschema.validate(resp_body, deuce_schema.file_list)

        self.assertListEqual(resp_body, [])

    def tearDown(self):
        super(TestEmptyFile, self).tearDown()
        [self.client.delete_file(vaultname=self.vaultname,
            fileid=file_info.Id) for file_info in self.files]
        self.client.delete_vault(self.vaultname)


class TestFileAssignedBlocks(base.TestBase):

    def setUp(self):
        super(TestFileAssignedBlocks, self).setUp()
        self.create_empty_vault()
        self.upload_multiple_blocks(3)
        self.create_new_file()
        self.assign_all_blocks_to_file()

    def test_finalize_file(self):
        """Finalize a file with some blocks assigned"""

        resp = self.client.finalize_file(filesize=self.filesize,
                                         alternate_url=self.fileurl)
        self.assertEqual(resp.status_code, 200,
                         'Status code returned: {0} . '
                         'Expected 200'.format(resp.status_code))
        self.assertHeaders(resp.headers, contentlength=0)
        self.assertEqual(len(resp.content), 0)

    def test_get_unfinalized_file(self):
        """Get a (unfinalized) file"""

        resp = self.client.get_file(self.vaultname, self.fileid)
        self.assert_409_response(resp)

        resp_body = resp.json()
        jsonschema.validate(resp_body, deuce_schema.error)

        self.assertEqual(resp_body['title'], 'Conflict')
        self.assertEqual(resp_body['description'], 'File not Finalized')

    def tearDown(self):
        super(TestFileAssignedBlocks, self).tearDown()
        [self.client.delete_file(vaultname=self.vaultname,
            fileid=file_info.Id) for file_info in self.files]
        [self.client.delete_block(self.vaultname, block.Id) for block in
            self.blocks]
        self.client.delete_vault(self.vaultname)


class TestFileMissingBlock(base.TestBase):

    def setUp(self):
        super(TestFileMissingBlock, self).setUp()
        self.create_empty_vault()
        self.upload_block()
        self.generate_block_data()
        self.upload_block()
        self.generate_block_data()
        self.upload_block()
        self.create_new_file()
        self.assign_all_blocks_to_file()

    def test_finalize_file_missing_block(self):
        """Finalize a file with some blocks missing"""

        resp = self.client.finalize_file(filesize=self.filesize,
                                         alternate_url=self.fileurl)
        self.assert_409_response(resp)

        # The response will only list the first missing block
        resp_body = resp.json()
        jsonschema.validate(resp_body, deuce_schema.error)

        expected = '"[{0}\\\\{1}] Gap in file {2} from {3}-{4}"'
        self.assertEqual(resp_body['title'], 'Conflict')
        self.assertEqual(resp_body['description'], expected.format(
            self.client.default_headers['X-Project-Id'], self.vaultname,
            self.fileid, 30720, 30720 * 2))

    def tearDown(self):
        super(TestFileMissingBlock, self).tearDown()
        [self.client.delete_file(vaultname=self.vaultname,
            fileid=file_info.Id) for file_info in self.files]
        [self.client.delete_block(self.vaultname, block.Id) for block in
            self.blocks]
        self.client.delete_vault(self.vaultname)


class TestFileOverlappingBlock(base.TestBase):

    def setUp(self):
        super(TestFileOverlappingBlock, self).setUp()
        self.create_empty_vault()
        self.upload_multiple_blocks(4)
        self.create_new_file()
        # Assign the files but set the offset to half the size of the block
        self.assign_all_blocks_to_file(offset_divisor=2)

    def test_finalize_file_overlapping_block(self):
        """Finalize a file with some blocks overlapping"""

        resp = self.client.finalize_file(filesize=self.filesize,
                                         alternate_url=self.fileurl)
        self.assert_409_response(resp)

        # The response will only list the first overlapping block
        resp_body = resp.json()
        jsonschema.validate(resp_body, deuce_schema.error)

        expected = '"[{0}/{1}] Overlap at block {2} file {3} at [{4}-{5}]"'
        self.assertEqual(resp_body['title'], 'Conflict')
        self.assertEqual(resp_body['description'], expected.format(
            self.client.default_headers['X-Project-Id'], self.vaultname,
            self.blocks[1].Id, self.fileid, 30720 / 2, 30720))

    def tearDown(self):
        super(TestFileOverlappingBlock, self).tearDown()
        [self.client.delete_file(vaultname=self.vaultname,
            fileid=file_info.Id) for file_info in self.files]
        [self.client.delete_block(self.vaultname, block.Id) for block in
            self.blocks]
        self.client.delete_vault(self.vaultname)


@ddt.ddt
class TestListBlocksOfFile(base.TestBase):

    def setUp(self):
        super(TestListBlocksOfFile, self).setUp()
        self.create_empty_vault()
        self.upload_multiple_blocks(20)
        self.blockids = []
        self.blockids_offsets = []
        offset = 0
        for block in self.blocks:
            self.blockids.append(block.Id)
            self.blockids_offsets.append((block.Id, offset))
            offset += len(block.Data)
        self.create_new_file()
        self.assign_all_blocks_to_file()

    def test_list_blocks_file(self):
        """List multiple blocks (20) assigned to the file"""

        resp = self.client.list_of_blocks_in_file(self.vaultname, self.fileid)
        self.assert_200_response(resp)

        resp_body = resp.json()
        jsonschema.validate(resp_body, deuce_schema.block_list_of_file)

        self.assertBlocksInResponse(resp_body)
        self.assertEqual(len(self.blockids_offsets), 0,
                         'Discrepancy between the list of blocks returned '
                         'and the blocks associated to the file')

    @ddt.data(2, 4, 5, 10)
    def test_list_blocks_file_marker(self, value):
        """List multiple blocks (20) assigned to the file using a marker
        (value)"""

        markerid = self.blockids_offsets[value][1]
        skipped_blockids_offsets = self.blockids_offsets[:value]
        resp = self.client.list_of_blocks_in_file(self.vaultname, self.fileid,
                                                  marker=markerid)
        self.assert_200_response(resp)

        resp_body = resp.json()
        jsonschema.validate(resp_body, deuce_schema.block_list_of_file)

        self.assertBlocksInResponse(resp_body)
        self.assertEqual(self.blockids_offsets, skipped_blockids_offsets,
                         'Discrepancy between the list of blocks returned '
                         'and the blocks associated to the file')

    @ddt.data(2, 4, 5, 10)
    def test_list_blocks_file_limit(self, value):
        """List multiple blocks in the file, setting the limit to value"""

        self.assertBlocksInFilePerPage(value)

    @ddt.data(2, 4, 5, 10)
    def test_list_blocks_file_limit_marker(self, value):
        """List multiple blocks in the file, setting the limit to value and
        using a marker. The marker is an offset"""

        markerid = self.blockids_offsets[value][1]
        self.assertBlocksInFilePerPage(value, marker=markerid, pages=1)

    def assertBlocksInFilePerPage(self, value, marker=None, pages=0):
        """
        Helper function to check the blocks in a file returned per request
        Also verifies that the marker, if provided, is used
        """

        url = None
        for i in range(20 / value - pages):
            if not url:
                resp = self.client.list_of_blocks_in_file(self.vaultname,
                                                          self.fileid,
                                                          marker=marker,
                                                          limit=value)
            else:
                resp = self.client.list_of_blocks_in_file(alternate_url=url)

            self.assert_200_response(resp)

            if i < 20 / value - (1 + pages):
                self.assertIn('x-next-batch', resp.headers)
                url = resp.headers['x-next-batch']
                self.assertUrl(url, fileblock=True, nextlist=True)
            else:
                self.assertNotIn('x-next-batch', resp.headers)

            resp_body = resp.json()
            jsonschema.validate(resp_body, deuce_schema.block_list_of_file)

            self.assertEqual(len(resp_body), value,
                             'Number of block ids returned is not {0} . '
                             'Returned {1}'.format(value, len(resp_body)))
            self.assertBlocksInResponse(resp_body)
        self.assertEqual(len(self.blockids_offsets), value * pages,
                         'Discrepancy between the list of blocks returned '
                         'and the blocks associated to the file')

    def assertBlocksInResponse(self, response):
        """Check the block information returned in the response"""

        for id_offset in response:
            self.assertIn(id_offset[0], self.blockids)
            i = self.blockids.index(id_offset[0])
            self.assertEqual(id_offset[0], self.blockids_offsets[i][0])
            self.assertEqual(id_offset[1], self.blockids_offsets[i][1])
            del self.blockids[i]
            del self.blockids_offsets[i]

    def tearDown(self):
        super(TestListBlocksOfFile, self).tearDown()
        [self.client.delete_file(vaultname=self.vaultname,
            fileid=file_info.Id) for file_info in self.files]
        [self.client.delete_block(self.vaultname, block.Id) for block in
            self.blocks]
        self.client.delete_vault(self.vaultname)


class TestFinalizedFile(base.TestBase):

    def setUp(self):
        super(TestFinalizedFile, self).setUp()
        self.create_empty_vault()
        self.upload_multiple_blocks(3)
        self.create_new_file()
        self.assign_all_blocks_to_file()
        self.finalize_file()

    def test_get_file(self):
        """Get a (finalized) file"""

        resp = self.client.get_file(self.vaultname, self.fileid)
        self.assertEqual(resp.status_code, 200,
                         'Status code returned: {0} . '
                         'Expected 200'.format(resp.status_code))
        self.assertHeaders(resp.headers, binary=True,
                           contentlength=self.filesize)
        filedata = ''
        for block in self.blocks:
            filedata += block.Data
        self.assertEqual(resp.content, filedata,
                         'Content of the file does not match was was expected')

    def test_delete_file(self):
        """Delete a (finalized) file"""

        resp = self.client.delete_file(self.vaultname, self.fileid)
        self.assert_204_response(resp)

    def test_list_finalized_file(self):
        """Get list of files with only one file that is finalized"""

        resp = self.client.list_of_files(vaultname=self.vaultname)
        self.assert_200_response(resp)

        resp_body = resp.json()
        jsonschema.validate(resp_body, deuce_schema.file_list)

        self.assertListEqual([self.fileid], resp_body)

    def test_assign_block_finalized_file(self):
        """Assign a block to a finalized file"""

        block_list = list()
        block_info = self.blocks[0]
        block_list.append([block_info.Id, 0])

        resp = self.client.assign_to_file(json.dumps(block_list),
                                          alternate_url=self.fileurl)

        self.assert_409_response(resp)

        resp_body = resp.json()
        jsonschema.validate(resp_body, deuce_schema.error)

        self.assertEqual(resp_body['title'], 'Conflict')
        self.assertEqual(resp_body['description'],
                         'Finalized file cannot be modified')

    def tearDown(self):
        super(TestFinalizedFile, self).tearDown()
        [self.client.delete_file(vaultname=self.vaultname,
            fileid=file_info.Id) for file_info in self.files]
        [self.client.delete_block(self.vaultname, block.Id) for block in
            self.blocks]
        self.client.delete_vault(self.vaultname)


@ddt.ddt
class TestMultipleFinalizedFiles(base.TestBase):

    def setUp(self):
        super(TestMultipleFinalizedFiles, self).setUp()
        self.create_empty_vault()
        self.blocks_file = []
        for _ in range(20):
            self.blocks = []
            self.create_new_file()
            self.upload_block()
            self.assign_all_blocks_to_file()
            self.blocks_file.append(*self.blocks)
            self.finalize_file()
        self.created_files = [file_info.Id for file_info in self.files]
        self.file_ids = self.created_files[:]

    def test_list_multiple_files(self):
        """List multiple files (20)"""

        resp = self.client.list_of_files(vaultname=self.vaultname)
        self.assert_200_response(resp)

        resp_body = resp.json()
        jsonschema.validate(resp_body, deuce_schema.file_list)

        self.assertListEqual(resp_body, sorted(self.file_ids))

    @ddt.data(2, 4, 5, 10)
    def test_list_multiple_files_marker(self, value):
        """List multiple files (20) using a marker (value)"""

        sorted_list_files = sorted(self.file_ids)
        markerid = sorted_list_files[value]
        requested_list_files = sorted_list_files[value:]
        resp = self.client.list_of_files(vaultname=self.vaultname,
                                         marker=markerid)
        self.assert_200_response(resp)

        resp_body = resp.json()
        jsonschema.validate(resp_body, deuce_schema.file_list)

        self.assertListEqual(resp_body, requested_list_files)

    @ddt.data(2, 4, 5, 10)
    def test_list_files_limit(self, value):
        """List multiple files, setting the limit to value"""

        self.assertFilesPerPage(value)

    @ddt.data(2, 4, 5, 10)
    def test_list_files_limit_marker(self, value):
        """List multiple files, setting the limit to value and using a
        marker"""

        markerid = sorted(self.file_ids)[value]
        self.assertFilesPerPage(value, marker=markerid, pages=1)

    def assertFilesPerPage(self, value, marker=None, pages=0):
        """
        Helper function to check the files returned per request
        Also verifies that the marker, if provided, is used
        """

        url = None
        for i in range(20 / value - pages):
            if not url:
                resp = self.client.list_of_files(self.vaultname,
                                                 marker=marker, limit=value)
            else:
                resp = self.client.list_of_files(alternate_url=url)

            self.assert_200_response(resp)

            if i < 20 / value - (1 + pages):
                self.assertIn('x-next-batch', resp.headers)
                url = resp.headers['x-next-batch']
                self.assertUrl(url, files=True, nextlist=True)
            else:
                self.assertNotIn('x-next-batch', resp.headers)

            resp_body = resp.json()
            jsonschema.validate(resp_body, deuce_schema.file_list)

            self.assertEqual(len(resp_body), value,
                             'Number of file ids returned is not {0} . '
                             'Returned {1}'.format(value, len(resp_body)))
            for fileid in resp_body:
                self.assertIn(fileid, self.file_ids)
                self.file_ids.remove(fileid)
        self.assertEqual(len(self.file_ids), value * pages,
                         'Discrepancy between the list of files returned '
                         'and the files created/finalilzed')

    def test_list_files_bad_marker(self):
        """Request File List with a bad marker"""

        fileids, fileurls = zip(*self.files)
        while True:
            bad_marker = uuid.uuid4()
            if bad_marker not in fileids:
                break
        resp = self.client.list_of_files(self.vaultname, marker=bad_marker)
        self.assert_404_response(resp)

    def tearDown(self):
        super(TestMultipleFinalizedFiles, self).tearDown()
        [self.client.delete_file(vaultname=self.vaultname,
            fileid=fileid) for fileid in self.created_files]
        [self.client.delete_block(self.vaultname, block.Id) for block in
            self.blocks_file]
        self.client.delete_vault(self.vaultname)
