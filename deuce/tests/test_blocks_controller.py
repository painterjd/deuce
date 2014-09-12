import ddt
import hashlib
import json
import msgpack
import os
from random import randrange

from pecan import conf
from six.moves.urllib.parse import urlparse, parse_qs

from deuce.tests import ControllerTest


@ddt.ddt
class TestBlocksController(ControllerTest):

    def setUp(self):
        super(TestBlocksController, self).setUp()

        # Create a vault for us to work with

        vault_name = self.create_vault_id()
        self._vault_path = '/v1.0/vaults/{0}'.format(vault_name)
        self._blocks_path = '{0}/blocks'.format(self._vault_path)

        self._files_path = self._vault_path + '/files'
        self._hdrs = {"x-project-id": self.create_project_id()}

        response = self.app.put(self._vault_path,
                                headers=self._hdrs)

        self.block_list = []
        self.total_block_num = 0

    def test_no_block_state(self):
        # Try listing the blocks. There should be none
        response = self.app.get(self._blocks_path, headers=self._hdrs)
        assert response.json_body == []

        response = self.app.get(self._blocks_path, headers={
            "X-Username": "failing_auth_hook",
            "X-Password": "failing_auth_hook"},
            expect_errors=True)

    def test_get_all_with_trailing_slash(self):
        path = self.get_block_path('')

        response = self.app.get(path, headers=self._hdrs,
                                expect_errors=True)

        assert response.status_int == 404

    def test_get_all_invalid_vault_id(self):
        path = '/v1.0/vaults/{0}/blocks'.format('bad_vault_id')
        response = self.app.get(path, headers=self._hdrs,
                                expect_errors=True)

        self.assertEqual(response.status_int, 404)

    def test_put_invalid_block_id(self):
        path = self.get_block_path('invalid_block_id')

        response = self.app.put(path, headers=self._hdrs,
                                expect_errors=True)

        self.assertEqual(response.status_int, 400)

        # Put a block with the invalid blockid/hash.
        path = self.get_block_path('1234567890123456789012345678901234567890')
        headers = {
            "Content-Type": "application/octet-stream",
            "Content-Length": str(10),
        }
        headers.update(self._hdrs)
        data = os.urandom(10)
        response = self.app.put(path, headers=headers,
                                params=data, expect_errors=True)
        self.assertEqual(response.status_int, 412)

    def test_post_invalid_block_id(self):
        path = self.get_block_path(self._blocks_path)

        response = self.app.post(path, headers=self._hdrs,
                                 expect_errors=True)

        self.assertEqual(response.status_int, 404)

        # Post several blocks with the invalid blockid/hash.
        headers = {
            "Content-Type": "application/msgpack",
        }
        data = [os.urandom(10)]
        block_list = [hashlib.sha1(b'mock').hexdigest()]
        headers.update(self._hdrs)
        contents = dict(zip(block_list, data))

        request_body = msgpack.packb(contents)
        response = self.app.post(self._blocks_path, headers=headers,
                                 params=request_body, expect_errors=True)
        self.assertEqual(response.status_int, 412)

    def test_post_invalid_request_body(self):
        path = self.get_block_path(self._blocks_path)

        # Post several blocks with invalid request body
        headers = {
            "Content-Type": "application/msgpack",
        }
        data = os.urandom(10)
        block_list = hashlib.sha1(b'mock').hexdigest()
        headers.update(self._hdrs)
        contents = [block_list, data] * 3

        request_body = msgpack.packb(contents)
        response = self.app.post(self._blocks_path, headers=headers,
                                 params=request_body, expect_errors=True)
        self.assertEqual(response.status_int, 400)

        # Post non-message packed request body
        response = self.app.post(self._blocks_path, headers=headers,
                                 params='non-msgpack', expect_errors=True)
        self.assertEqual(response.status_int, 400)

    def test_post_invalid_endpoint(self):
            path = self.get_block_path(self._blocks_path)

            headers = {
                "Content-Type": "application/msgpack",
            }
            headers.update(self._hdrs)
            data = [os.urandom(x) for x in range(3)]
            block_list = [self.calc_sha1(d) for d in data]

            contents = dict(zip(block_list, data))

            request_body = msgpack.packb(contents)
            # invalid endpoint : POST v1.0/vaults/{vault_name}/blocks/myblock
            response = self.app.post(self._blocks_path + '/myblock',
                                     headers=headers,
                                     params=request_body, expect_errors=True)
            self.assertEqual(response.status_int, 404)
            # invalid endpoint : POST v1.0/vaults/{vault_name}/blocks/myblock
            # with no request_body
            response = self.app.post(self._blocks_path + '/myblock',
                                     headers=headers,
                                     expect_errors=True)
            self.assertEqual(response.status_int, 404)

    def test_with_bad_marker_and_limit(self):
        block_list = self.helper_create_blocks(num_blocks=5)

        # TODO: Need reenable after each function can cleanup/delete
        #       blocks afterward.
        # Now try to get a list of blocks to ensure that they'e
        # there.
        # resp = self.app.get(self._blocks_path, headers=self._hdrs)
        # all_blocks = resp.json_body
        # self.assertEqual(len(all_blocks), 5)
        # self.assertEqual(resp.status_code, 200)

        # Now check the first one. We're going to send the marker
        # and limit and we should get just one
        args = dict(limit=1)

        resp = self.app.get(self._blocks_path, params=args,
                            headers=self._hdrs)

        self.assertEqual(len(resp.json_body), 1)
        self.assertEqual(resp.status_code, 200)

        # Now try with a bad limit
        args = dict(limit='blah')

        resp = self.app.get(self._blocks_path, params=args,
                            headers=self._hdrs, expect_errors=True)

        self.assertEqual(resp.status_code, 404)

        # Now try a bad marker
        args = dict(marker='blah')
        resp = self.app.get(self._blocks_path, params=args,
                            headers=self._hdrs, expect_errors=True)

        self.assertEqual(resp.status_code, 404)

    @ddt.data(True, False)
    def test_put_and_list(self, async_status):

        # Test None block_id
        path = '{0}/'.format(self._blocks_path)
        data = os.urandom(100)
        headers = {
            "Content-Type": "application/octet-stream",
            "Content-Length": "100"
        }
        response = self.app.put(path, headers=headers,
            params=data, expect_errors=True)
        # Returns from BlockPutRuleNoneOk validation.
        self.assertEqual(response.status_code, 400)

        # Create 5 blocks
        block_list = self.helper_create_blocks(num_blocks=5,
                                               async=async_status)
        self.total_block_num = 5
        self.block_list += block_list

        # List all.
        next_batch_url = self.helper_get_blocks(self._blocks_path,
            0, 0, assert_ret_url=False, assert_data_len=5,
            repeat=False, exam_block_data=True)

        # List some blocks
        next_batch_url = self.helper_get_blocks(self._blocks_path,
                                                0, 4, True, 4, False)

        # List the rest blocks
        marker = parse_qs(urlparse(next_batch_url).query)['marker']
        next_batch_url = self.helper_get_blocks(self._blocks_path,
                                                marker, 8, False, 1, False)

        # Create more blocks.
        num_blocks = int(1.5 * conf.api_configuration.max_returned_num)
        block_list = self.helper_create_blocks(num_blocks=num_blocks)
        self.block_list += block_list
        self.total_block_num += num_blocks

        # List from 0; use conf limit
        next_batch_url = self.helper_get_blocks(self._blocks_path,
            0, 0, assert_ret_url=True,
            assert_data_len=conf.api_configuration.max_returned_num,
            repeat=False)

        # List from 0; Use conf limit, repeat to the end.
        next_batch_url = self.helper_get_blocks(self._blocks_path,
            0, 0, assert_ret_url=False,
            assert_data_len=self.total_block_num, repeat=True)

        # Try to get some blocks that don't exist. This should
        # result in 404s
        bad_block_ids = [self.create_block_id() for _ in range(0, 5)]

        for bad_id in bad_block_ids:
            path = self.get_block_path(bad_id)

            response = self.app.get(path, headers=self._hdrs,
                                    expect_errors=True)

            self.assertEqual(response.status_int, 404)

    def test_delete_blocks_validation(self):
        # delete non existent block
        response = self.app.delete(self.get_block_path(
                                   self.create_block_id()),
                                   headers=self._hdrs,
                                   expect_errors=True)
        self.assertEqual(response.status_int, 404)

        # delete block from non existent vault
        response = self.app.delete('/v1.0/vaults/blah/blocks/' +
                                   self.create_block_id(),
                                   headers=self._hdrs,
                                   expect_errors=True)
        self.assertEqual(response.status_int, 404)

    def test_delete_blocks_no_references(self):
        # Just create and delete blocks
        blocklist = self.helper_create_blocks(10)
        for block in blocklist:
            response = self.app.delete(self.get_block_path(block),
                                       headers=self._hdrs)
            self.assertEqual(response.status_int, 204)

    @ddt.data(True, False)
    def test_delete_blocks_with_references(self, finalize_status):

        # Create two files each consisting of 3 blocks of size 100 bytes

        responses = [self.app.post(self._files_path, headers=self._hdrs)
                     for _ in range(2)]
        file_ids = [urlparse(response.headers["Location"]).path
                    for response in responses]
        block_list = self.helper_create_blocks(3, singleblocksize=True)

        offsets = [x * 100 for x in range(3)]
        meta_info = [{'id': block, 'size': 100, 'offset': offset}
                     for block, offset in zip(block_list, offsets)]
        data = {"blocks": meta_info}

        hdrs = {'content-type': 'application/x-deuce-block-list'}
        hdrs.update(self._hdrs)

        for file_id in file_ids:
            # assign blocks to file
            response = self.app.post(file_id,
                params=json.dumps(data), headers=hdrs)
            if finalize_status:
                # finalize file
                filelength = {'x-file-length': '300'}
                hdrs.update(filelength)
                response = self.app.post(file_id, headers=hdrs)

        for block in block_list:
            response = self.app.delete(self.get_block_path(block),
                                headers=self._hdrs, expect_errors=True)
            self.assertEqual(response.status_int, 412)

    def helper_create_blocks(self, num_blocks, async=False,
                             singleblocksize=False, blocksize=100):
        min_size = 1
        max_size = 2000
        if singleblocksize:
            block_sizes = [blocksize for _ in range(num_blocks)]
        else:
            block_sizes = [randrange(min_size, max_size) for x in
                       range(0, num_blocks)]

        data = [os.urandom(x) for x in block_sizes]
        block_list = [self.calc_sha1(d) for d in data]

        block_data = zip(block_sizes, data, block_list)
        if async:
            contents = dict(zip(block_list, data))
            request_body = msgpack.packb(contents)
            headers = {
                "Content-Type": "application/msgpack"
            }
            headers.update(self._hdrs)
            response = self.app.post(self._blocks_path, headers=headers,
                                     params=request_body)
        else:

            # Put each one of the generated blocks on the
            # size
            for size, data, sha1 in block_data:
                path = self.get_block_path(sha1)

                # NOTE: Very important to set the content-type
                # header. Otherwise pecan tries to do a UTF-8 test.
                headers = {
                    "Content-Type": "application/octet-stream",
                    "Content-Length": str(size),
                }

                headers.update(self._hdrs)

                response = self.app.put(path, headers=headers,
                                        params=data)

        return block_list

    def helper_get_blocks(self, path, marker, limit, assert_ret_url,
              assert_data_len, repeat=False, exam_block_data=False):

        resp_block_list = []

        params = dict()

        if limit != 0:
            params['limit'] = limit

        if marker != 0:
            params['marker'] = marker

        while True:
            response = self.app.get(path,
                                    params=params, headers=self._hdrs)

            next_batch_url = response.headers.get("X-Next-Batch")

            resp_block_list += response.json_body
            assert isinstance(response.json_body, list)

            if not repeat:
                self.assertEqual(not next_batch_url, not assert_ret_url)
                self.assertEqual(len(resp_block_list), assert_data_len)
                for h in resp_block_list:
                    assert h in self.block_list
                if assert_data_len == -1 or \
                        assert_data_len == self.total_block_num:
                    for h in self.block_list:
                        assert h in resp_block_list
                if exam_block_data:
                    self.helper_exam_block_data(resp_block_list)
                return next_batch_url
            if not next_batch_url:
                break
            params['marker'] = \
                parse_qs(urlparse(next_batch_url).query)['marker']
        assert len(resp_block_list) == assert_data_len
        for h in resp_block_list:
            assert h in self.block_list
        for h in self.block_list:
            assert h in resp_block_list
        # By default exam blocks if fetching all blocks
        self.helper_exam_block_data(resp_block_list)

    def helper_exam_block_data(self, block_list):
        # Now try to fetch each block, and compare against
        # the original block data
        for sha1 in block_list:
            path = self.get_block_path(sha1)
            response = self.app.get(path, headers=self._hdrs)
            self.assertEqual(response.status_int, 200)
            self.assertIn('x-block-reference-count', response.headers)

            bindata = response.body

            # Now re-hash the data, the data that
            # was returned should match the original
            # sha1
            z = hashlib.sha1()
            z.update(bindata)
            assert z.hexdigest() == sha1
