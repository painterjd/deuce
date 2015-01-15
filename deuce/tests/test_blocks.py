import hashlib
import json
import os
from random import randrange
import uuid

import ddt
import falcon
from mock import patch
import msgpack
from six.moves.urllib.parse import urlparse, parse_qs

from deuce import conf
from deuce.util.misc import set_qs, relative_uri
from deuce.tests import ControllerTest


@ddt.ddt
class TestBlocksController(ControllerTest):

    def setUp(self):
        super(TestBlocksController, self).setUp()

        # Create a vault for us to work with
        self.vault_name = self.create_vault_id()

        self._hdrs = {"x-project-id": self.create_project_id()}

        response = self.simulate_put(self.get_vault_path(self.vault_name),
                                     headers=self._hdrs)

        self.block_list = []
        self.total_block_num = 0

    def test_no_block_state(self):
        # Try listing the blocks. There should be none
        response = self.simulate_get(self.get_blocks_path(self.vault_name),
                                     headers=self._hdrs)
        self.assertEqual(response[0].decode(), json.dumps([]))

    def test_get_all_with_trailing_slash(self):
        path = self.get_block_path(self.vault_name, '')

        response = self.simulate_get(path, headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_200)

    def test_get_all_invalid_vault_id(self):
        path = '/v1.0/vaults/{0}/blocks'.format('bad_vault_id')
        response = self.simulate_get(path, headers=self._hdrs)

        self.assertEqual(self.srmock.status, falcon.HTTP_404)

    def test_get_invalid_block_id(self):
        path = self.get_block_path(self.vault_name, 'invalid_block_id')
        response = self.simulate_get(path, headers=self._hdrs)

        self.assertEqual(self.srmock.status, falcon.HTTP_404)

    def test_get_invalid_block_id_swift_failure(self):
        block_list = self.helper_create_blocks(1, async=True)[0]
        data = os.urandom(100)
        block_id = self.calc_sha1(data)

        path = self.get_block_path(self.vault_name, block_id)
        response = self.simulate_get(path, headers=self._hdrs)

        self.assertEqual(self.srmock.status, falcon.HTTP_404)

    def test_get_inconsistent_metadata_block_id(self):
        block_list = self.helper_create_blocks(1, async=True)[0]
        data = os.urandom(100)
        block_id = self.calc_sha1(data)

        from deuce.model import Vault
        with patch.object(Vault, '_meta_has_block', return_value=True):
            path = self.get_block_path(self.vault_name, block_id)
            self.simulate_get(path, headers=self._hdrs)
            self.assertEqual(self.srmock.status, falcon.HTTP_410)
            self.assertIn('x-block-id', str(self.srmock.headers))
            self.assertIn('x-storage-id', str(self.srmock.headers))
            self.assertIn('x-ref-modified', str(self.srmock.headers))
            self.assertIn('x-block-reference-count', str(self.srmock.headers))

    def test_head_invalid_block_id(self):
        path = self.get_block_path(self.vault_name, 'invalid_block_id')
        response = self.simulate_get(path, headers=self._hdrs)

        self.assertEqual(self.srmock.status, falcon.HTTP_404)

    def test_head_non_existent_block_id(self):
        path = self.get_block_path(self.vault_name, self.calc_sha1(b'mock'))
        response = self.simulate_head(path, headers=self._hdrs)

        self.assertEqual(self.srmock.status, falcon.HTTP_404)

    def test_head_inconsistent_metadata_block_id(self):
        from deuce.model import Vault
        with patch.object(Vault, '_storage_has_block', return_value=False):
            block_list = self.helper_create_blocks(1, async=True)[0]
            path = self.get_block_path(self.vault_name, block_list[0])
            self.simulate_head(path, headers=self._hdrs)
            self.assertEqual(self.srmock.status, falcon.HTTP_410)
            self.assertIn('x-block-id', str(self.srmock.headers))
            self.assertIn('x-storage-id', str(self.srmock.headers))
            self.assertIn('x-ref-modified', str(self.srmock.headers))
            self.assertIn('x-block-reference-count', str(self.srmock.headers))

    def test_head_block_nonexistent_vault(self):
        self.simulate_head('/v1.0/vaults/mock/blocks/'
                           + self.calc_sha1(b'mock'), headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_404)

    def test_head_block(self):
        block_list = self.helper_create_blocks(1, async=True)[0]
        path = self.get_block_path(self.vault_name, block_list[0])
        self.simulate_head(path, headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_204)
        self.assertIn('x-block-reference-count', str(self.srmock.headers))
        self.assertIn('x-ref-modified', str(self.srmock.headers))
        self.assertIn('x-storage-id', str(self.srmock.headers))
        resp_sha1, resp_uuid = self.srmock.headers_dict['x-storage-id'].\
            split('_')
        self.assertTrue(uuid.UUID(resp_uuid))
        self.assertIn('x-block-id', str(self.srmock.headers))
        self.assertEqual(block_list[0], self.srmock.headers_dict['x-block-id'])

    def test_put_invalid_block_id(self):
        path = self.get_block_path(self.vault_name, 'invalid_block_id')

        response = self.simulate_put(path, headers=self._hdrs)

        self.assertEqual(self.srmock.status, falcon.HTTP_400)

        # Put a block with the invalid blockid/hash.
        path = self.get_block_path(self.vault_name,
                                   '1234567890123456789012345678901234567890')
        headers = {
            "Content-Type": "application/octet-stream",
            "Content-Length": str(10),
        }
        headers.update(self._hdrs)
        data = os.urandom(10)
        response = self.simulate_put(path, headers=headers, body=data)
        self.assertEqual(self.srmock.status, falcon.HTTP_412)

    def test_put_invalid_length(self):

        data = os.urandom(100)
        blockid = self.calc_sha1(data)
        blocklen = 5

        path = self.get_block_path(self.vault_name, blockid)
        headers = {
            "Content-Type": "application/octet-stream",
            "Content-Length": str(blocklen),
        }
        headers.update(self._hdrs)
        response = self.simulate_put(path, headers=headers, body=data)
        self.assertEqual(self.srmock.status, falcon.HTTP_412)

    def test_put_happy_case(self):

        block_list = self.helper_create_blocks(num_blocks=1)[0]
        self.assertEqual(len(block_list), 1)

        self.assertEqual(self.srmock.status, falcon.HTTP_201)
        self.assertIn('x-storage-id', self.srmock.headers_dict)
        self.assertIn('x-block-id', self.srmock.headers_dict)
        self.assertEqual(block_list[0], self.srmock.headers_dict['x-block-id'])
        self.assertNotEqual(0, self.srmock.headers_dict['x-storage-id'])

    def test_post_invalid_block_id(self):
        path = self.get_block_path(self.vault_name,
                                   self.get_blocks_path(self.vault_name))

        response = self.simulate_post(path, headers=self._hdrs)

        self.assertEqual(self.srmock.status, falcon.HTTP_404)

        # Post several blocks with the invalid blockid/hash.
        headers = {
            "Content-Type": "application/msgpack",
        }
        data = [os.urandom(10)]
        block_list = [hashlib.sha1(b'mock').hexdigest()]
        headers.update(self._hdrs)
        contents = dict(zip(block_list, data))

        request_body = msgpack.packb(contents)
        response = self.simulate_post(self.get_blocks_path(self.vault_name),
                                      headers=headers,
                                      body=request_body)
        self.assertEqual(self.srmock.status, falcon.HTTP_412)

    def test_post_invalid_request_body(self):
        path = self.get_blocks_path(self.vault_name)

        # Post several blocks with invalid request body
        headers = {
            "Content-Type": "application/msgpack",
        }
        data = os.urandom(10)
        block_list = hashlib.sha1(b'mock').hexdigest()
        headers.update(self._hdrs)
        contents = [block_list, data] * 3

        request_body = msgpack.packb(contents)
        response = self.simulate_post(path,
                                      headers=headers,
                                      body=request_body)
        self.assertEqual(self.srmock.status, falcon.HTTP_400)

        # Post non-message packed request body
        response = self.simulate_post(path,
                                      headers=headers,
                                      body='non-msgpack')
        self.assertEqual(self.srmock.status, falcon.HTTP_400)

    def test_post_invalid_endpoint(self):
        path = self.get_blocks_path(self.vault_name)

        headers = {
            "Content-Type": "application/msgpack",
        }
        headers.update(self._hdrs)
        data = [os.urandom(x) for x in range(3)]
        block_list = [self.calc_sha1(d) for d in data]

        contents = dict(zip(block_list, data))

        request_body = msgpack.packb(contents)
        # invalid endpoint : POST v1.0/vaults/{vault_name}/blocks/myblock
        response = self.simulate_post(path + '/myblock',
                                      headers=headers,
                                      body=request_body)
        self.assertEqual(self.srmock.status, falcon.HTTP_405)
        # invalid endpoint : POST v1.0/vaults/{vault_name}/blocks/myblock
        # with no request_body
        response = self.simulate_post(path + '/myblock',
                                      headers=headers)
        self.assertEqual(self.srmock.status, falcon.HTTP_405)

    def test_with_bad_marker_and_limit(self):
        block_list = self.helper_create_blocks(5)[0]

        # TODO: Need reenable after each function can cleanup/delete
        # blocks afterward.
        # Now try to get a list of blocks to ensure that they'e
        # there.
        # response = self.simulate_get(self.get_blocks_path(self.vault_name),
        #                              headers=self._hdrs)
        # all_blocks = response[0].decode()
        # self.assertEqual(len(all_blocks), 5)
        # self.assertEqual(self.srmock.status, falcon.HTTP_200)
        #
        # Now check the first one. We're going to send the marker
        # and limit and we should get just one

        response = self.simulate_get(self.get_blocks_path(self.vault_name),
                                     query_string='limit=1',
                                     headers=self._hdrs)

        self.assertEqual(len(response), 1)
        self.assertEqual(self.srmock.status, falcon.HTTP_200)

        # Now try with a bad limit

        response = self.simulate_get(self.get_blocks_path(self.vault_name),
                                     query_string='limit=blah',
                                     headers=self._hdrs)

        self.assertEqual(self.srmock.status, falcon.HTTP_404)

        # Now try a bad marker

        response = self.simulate_get(self.get_blocks_path(self.vault_name),
                                     query_string='marker=blah',
                                     headers=self._hdrs)

        self.assertEqual(self.srmock.status, falcon.HTTP_404)

    @ddt.data(True, False)
    def test_put_and_list(self, async_status):
        # Test None block_id
        path = '{0}/'.format(self.get_blocks_path(self.vault_name))
        data = os.urandom(100)
        headers = {
            "Content-Type": "application/octet-stream",
            "Content-Length": "100"
        }
        response = self.simulate_put(path, headers=headers,
                                     body=data)

        #  (BenjamenMeyer) Since we don't specify a block id
        # this will go against the collection and thus be an
        # HTTP 405 Error - Method Not Allowed - as opposed to
        # an HTTP 400 error.
        self.assertEqual(self.srmock.status, falcon.HTTP_405)

        # Create 5 blocks
        block_list, response = self.helper_create_blocks(num_blocks=5,
                                                         async=async_status)
        self.total_block_num = 5
        self.block_list += block_list

        # verify all blocks in the block list also have an entry in
        # the response list
        if async_status is True:
            self.helper_exam_block_metadata(block_list, response)

        # List all.
        next_batch_url = self.helper_get_blocks(path,
                                                0, 0, assert_ret_url=False,
                                                assert_data_len=5,
                                                repeat=False,
                                                exam_block_data=True)

        # List some blocks
        next_batch_url = self.helper_get_blocks(path,
                                                0, 4, True, 4, False)

        # List the rest blocks
        # TODO (TheSriram): Make finding marker more elegant
        marker = next_batch_url.split('marker=')[1]
        next_batch_url = self.helper_get_blocks(path,
                                                marker, 8, False, 1, False)

        # Create more blocks.
        num_blocks = int(1.5 * conf.api_configuration.default_returned_num)
        block_list = self.helper_create_blocks(num_blocks=num_blocks)[0]
        self.block_list += block_list
        self.total_block_num += num_blocks

        # List from 0; use conf limit
        max_num = conf.api_configuration.default_returned_num
        next_batch_url = self.helper_get_blocks(path,
                                                0, 0, assert_ret_url=True,
                                                assert_data_len=max_num,
                                                repeat=False)

        # List from 0; Use conf limit, repeat to the end.
        block_num = self.total_block_num
        next_batch_url = self.helper_get_blocks(path,
                                                0, 0, assert_ret_url=False,
                                                assert_data_len=block_num,
                                                repeat=True)

        # Try to get some blocks that don't exist. This should
        # result in 404s
        bad_block_ids = [self.create_block_id() for _ in range(0, 5)]

        for bad_id in bad_block_ids:
            path = self.get_block_path(self.vault_name, bad_id)

            response = self.simulate_get(path, headers=self._hdrs)

            self.assertEqual(self.srmock.status, falcon.HTTP_404)

    def test_delete_blocks_validation(self):
        # delete non existent block
        response = self.simulate_delete(self.get_block_path(self.vault_name,
                                        self.create_block_id()),
                                        headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_404)

        # delete block from non existent vault
        response = self.simulate_delete('/v1.0/vaults/blah/blocks/' +
                                        self.create_block_id(),
                                        headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_404)

    def test_delete_blocks_no_references(self):
        # Just create and delete blocks
        blocklist = self.helper_create_blocks(10)[0]
        for block in blocklist:
            response = self.simulate_delete(
                self.get_block_path(self.vault_name, block),
                headers=self._hdrs)
            self.assertEqual(self.srmock.status, falcon.HTTP_204)

    @ddt.data(True, False)
    def test_delete_blocks_with_references(self, finalize_status):
        # Create two files each consisting of 3 blocks of size 100 bytes
        file_ids = []
        for _ in range(2):
            response = self.simulate_post(self.get_files_path(self.vault_name),
                                          headers=self._hdrs)
            rel_url, querystring = relative_uri(
                self.srmock.headers_dict['Location'])
            file_ids.append(rel_url)
        block_list = self.helper_create_blocks(3, singleblocksize=True)[0]

        offsets = [x * 100 for x in range(3)]
        data = list(zip(block_list, offsets))

        hdrs = {'content-type': 'application/x-deuce-block-list'}
        hdrs.update(self._hdrs)
        for file_id in file_ids:
            # assign blocks to file
            response = self.simulate_post(file_id + '/blocks',
                                          body=json.dumps(data), headers=hdrs)
            if finalize_status:
                # finalize file
                filelength = {'x-file-length': '300'}
                hdrs.update(filelength)
                response = self.simulate_post(file_id, headers=hdrs)

        for block in block_list:
            response = self.simulate_delete(
                self.get_block_path(self.vault_name, block),
                headers=self._hdrs)
            self.assertEqual(self.srmock.status, falcon.HTTP_409)

    def test_vault_error(self):
        from deuce.model import Vault
        with patch.object(Vault,
                          'put_async_block',
                          return_value=(False, [])):
            self.helper_create_blocks(1, async=True)
            self.assertEqual(self.srmock.status, falcon.HTTP_500)

    def test_vault_storage_failure_error(self):
        import deuce
        from deuce.model import Vault

        with patch.object(deuce.storage_driver,
                          'store_block',
                          return_value=(False, '')):
            self.helper_create_blocks(1, async=False)
            self.assertEqual(self.srmock.status, falcon.HTTP_500)

    def test_vault_async_storage_failure_error(self):
        import deuce
        from deuce.model import Vault

        with patch.object(deuce.storage_driver,
                          'store_async_block',
                          return_value=(False, [])):
            self.helper_create_blocks(1, async=True)
            self.assertEqual(self.srmock.status, falcon.HTTP_500)

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
        response = None
        if async:
            contents = dict(zip(block_list, data))
            request_body = msgpack.packb(contents)
            headers = {
                "Content-Type": "application/msgpack"
            }
            headers.update(self._hdrs)
            response = self.simulate_post(
                self.get_blocks_path(self.vault_name),
                headers=headers,
                body=request_body)
        else:

            # Put each one of the generated blocks on the
            # size
            for size, data, sha1 in block_data:
                path = self.get_block_path(self.vault_name, sha1)

                headers = {
                    "Content-Type": "application/octet-stream",
                    "Content-Length": str(size),
                }

                headers.update(self._hdrs)

                response = self.simulate_put(path, headers=headers,
                                             body=data)

        return (block_list, response)

    def helper_get_blocks(self, path, marker, limit, assert_ret_url,
                          assert_data_len, repeat=False,
                          exam_block_data=False):

        resp_block_list = []

        params = dict()

        if limit != 0:
            params['limit'] = limit

        if marker != 0:
            params['marker'] = marker

        while True:
            response = self.simulate_get(path,
                                         query_string=set_qs(path, params),
                                         headers=self._hdrs)

            next_batch = self.srmock.headers_dict.get("X-Next-Batch")
            if next_batch:
                next_batch_url, query_string = relative_uri(next_batch)
            else:
                next_batch_url = next_batch

            resp_block_list += json.loads(response[0].decode())
            self.assertIsInstance(json.loads(response[0].decode()), list)

            if not repeat:
                self.assertEqual(not next_batch_url, not assert_ret_url)
                self.assertEqual(len(resp_block_list), assert_data_len)
                for h in resp_block_list:
                    self.assertIn(h, self.block_list)
                if assert_data_len == -1 or \
                        assert_data_len == self.total_block_num:
                    for h in self.block_list:
                        self.assertIn(h, resp_block_list)
                if exam_block_data:
                    self.helper_exam_block_data(resp_block_list)
                return next_batch
            if not next_batch_url:
                break
            # TODO (TheSriram): Make finding marker more elegant
            for query in query_string.split('&'):
                if 'marker' in query:
                    current_marker = query.split('marker=')[1]
            params['marker'] = current_marker
        self.assertEqual(len(resp_block_list), assert_data_len)
        for h in resp_block_list:
            self.assertIn(h, self.block_list)
        for h in self.block_list:
            self.assertIn(h, resp_block_list)
        # By default exam blocks if fetching all blocks
        self.helper_exam_block_data(resp_block_list)

    def helper_exam_block_data(self, block_list):
        # Now try to fetch each block, and compare against
        # the original block data
        for sha1 in block_list:
            path = self.get_block_path(self.vault_name, sha1)
            response = self.simulate_get(path, headers=self._hdrs)

            self.assertEqual(self.srmock.status, falcon.HTTP_200)
            self.assertIn('x-block-reference-count', str(self.srmock.headers))

            # Now re-hash the data, the data that
            # was returned should match the original
            # sha1
            z = hashlib.sha1()
            z.update(response.read())
            self.assertEqual(z.hexdigest(), sha1)

    def helper_exam_block_metadata(self, block_list, upload_response):
        # Now verify each uploaded block is there
        uploaded_data = json.loads(upload_response[0].decode())
        for sha1 in block_list:
            self.assertIn(sha1, uploaded_data)

            path = self.get_block_path(self.vault_name, sha1)
            response = self.simulate_get(path, headers=self._hdrs)

            self.assertEqual(self.srmock.status, falcon.HTTP_200)
            self.assertIn('x-block-reference-count', str(self.srmock.headers))
            # ensure each reported x-storage-id matches between the
            # upload and download operations
            self.assertIn('x-storage-id', str(self.srmock.headers))
            self.assertEqual(uploaded_data[sha1],
                             self.srmock.headers_dict['x-storage-id'])

            # Now re-hash the data, the data that
            # was returned should match the original
            # sha1
            z = hashlib.sha1()
            z.update(response.read())
            self.assertEqual(z.hexdigest(), sha1)
