import hashlib
import json
import os
from random import randrange
import time

import falcon
import mock
from mock import patch
from six.moves.urllib.parse import urlparse, parse_qs

from deuce import conf
import deuce
from deuce.tests import ControllerTest
from deuce.util.misc import set_qs, relative_uri


class TestFiles(ControllerTest):

    def setUp(self):
        super(TestFiles, self).setUp()

        self.file_list = []
        self.max_ret_num = conf.api_configuration.default_returned_num
        self.total_file_num = 0

        self._hdrs = {"x-project-id": self.create_project_id()}

        # Create a vault and a file for us to work with
        self.vault_id = self.create_vault_id()
        self._vault_path = '/v1.0/vaults/' + self.vault_id
        self._files_path = self._vault_path + '/files'
        self._blocks_path = self._vault_path + '/blocks'

        # Create Vault
        response = self.simulate_put(self._vault_path, headers=self._hdrs)
        # Create File
        response = self.simulate_post(self._files_path, headers=self._hdrs)
        self._file_id = self.srmock.headers_dict['x-file-id']
        self._file_url = self.srmock.headers_dict['location']
        self._file_path = urlparse(self._file_url).path
        self._fileblocks_path = self._file_path + '/blocks'
        # Now, _file_id is '/v1.0/vaults/files_vault_test/files/SOME_FILE_ID'
        self.assertTrue(self._file_path.endswith(self._file_id))

        # Create distractor File
        response = self.simulate_post(self._files_path, headers=self._hdrs)
        self._distractor_file_id = self.srmock.headers_dict['x-file-id']
        self._distractor_url = self.srmock.headers_dict['location']
        self._distractor_file_path = urlparse(self._distractor_url).path
        self._distractor_fileblocks_path = self._distractor_file_path + \
            '/blocks'
        self._NOT_EXIST_files_path = '/v1.0/vaults/not_exists/files'

    def test_tenancy_requirement(self):
        # vault does not exists
        # If we pass in no headers, we should get a 400 back
        response = self.simulate_get(self._NOT_EXIST_files_path)
        self.assertEqual(self.srmock.status, falcon.HTTP_400)

        # vault does not exists
        response = self.simulate_get(self._NOT_EXIST_files_path,
                                     headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_404)

        # Delete a file from non existed vault.
        response = self.simulate_delete(self._NOT_EXIST_files_path + '/' +
                                        '10000000-0000-0000-0000-00000000000',
                                        headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_404)

        # Delete a non existed file.
        response = self.simulate_delete(self._files_path + '/' +
                                        '10000000-0000-0000-0000-00000000000',
                                        headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_404)

    def test_get_all(self):
        # Create few (< max_returned_num) files in the vault
        file_num = int(0.5 * self.max_ret_num)
        self.total_file_num = self.helper_create_files(file_num)

        # Get list of files in the vault with a given limit.
        next_batch_url = self.helper_get_files(marker=None,
                                               limit=(file_num - 1),
                                               assert_return_url=True,
                                               assert_data_len=(file_num - 1))

        # Get list of all files in the vault with the default limit.
        self.helper_get_files(marker=None, limit=None,
                              assert_return_url=False,
                              assert_data_len=self.total_file_num)
        # Add more (< max_returned_num) files in the vault
        file_num = int(0.5 * self.max_ret_num) - 1
        self.total_file_num += self.helper_create_files(file_num)

        # Get list of all files in the vault.
        self.helper_get_files(marker=None, limit=None,
                              assert_return_url=False,
                              assert_data_len=self.total_file_num)

        # Add one more file
        self.total_file_num += self.helper_create_files(1)
        total_num = self.total_file_num
        # Get list of all files in the vault, which is exact one load.
        next_batch_url = self.helper_get_files(marker=None, limit=None,
                                               assert_return_url=False,
                                               assert_data_len=total_num)

        # Add more files to make the total files more than one load
        file_num = int(0.5 * self.max_ret_num)
        self.total_file_num += self.helper_create_files(file_num)

        # Get list of all files in the vault, need multiple runs,
        self.helper_get_files(marker=None, limit=None,
                              assert_return_url=False,
                              assert_data_len=self.total_file_num,
                              repeat=True)

    def test_bad_limit_and_marker(self):

        response = self.simulate_get(self._NOT_EXIST_files_path,
                                     query_string='marker=blah',
                                     headers=self._hdrs)

        self.assertEqual(self.srmock.status, falcon.HTTP_404)

        response = self.simulate_get(self._NOT_EXIST_files_path,
                                     query_string='limit=blah',
                                     headers=self._hdrs)

        self.assertEqual(self.srmock.status, falcon.HTTP_404)

        response = self.simulate_get(self._NOT_EXIST_files_path + '/' +
                                     self.create_file_id() + '/blocks',
                                     query_string='marker=blah',
                                     headers=self._hdrs)

        self.assertEqual(self.srmock.status, falcon.HTTP_404)

    def test_get_one(self):
        # vault does not exists
        response = self.simulate_get(self._NOT_EXIST_files_path,
                                     headers=self._hdrs)

        self.assertEqual(self.srmock.status, falcon.HTTP_404)

        response = self.simulate_get(self._NOT_EXIST_files_path + '/',
                                     headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_404)

        file_id = self.create_file_id()

        response = self.simulate_get(self._NOT_EXIST_files_path
                                     + '/' + file_id,
                                     headers=self._hdrs)

        self.assertEqual(self.srmock.status, falcon.HTTP_404)

        # fileid is not privded
        response = self.simulate_get(
            self._files_path +
            '/',
            headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_200)

        # fileid does not exists
        response = self.simulate_get(self._files_path + '/' + file_id,
                                     headers=self._hdrs)

        self.assertEqual(self.srmock.status, falcon.HTTP_404)

    def test_post_and_check_one_file(self):
        # vault does not exists
        response = self.simulate_post(self._NOT_EXIST_files_path,
                                      headers=self._hdrs)

        self.assertEqual(self.srmock.status, falcon.HTTP_404)

        response = self.simulate_post(self._NOT_EXIST_files_path + '/',
                                      headers=self._hdrs)

        self.assertEqual(self.srmock.status, falcon.HTTP_404)

        file_id = self.create_file_id()

        response = self.simulate_post(self._NOT_EXIST_files_path
                                      + '/' + file_id,
                                      headers=self._hdrs)

        self.assertEqual(self.srmock.status, falcon.HTTP_400)

        # fileid is not provided.
        response = self.simulate_post(self._files_path + '/',
                                      headers=self._hdrs)

        self.assertEqual(self.srmock.status, falcon.HTTP_201)

        # fileid does not exists
        response = self.simulate_post(self._files_path + '/' +
                                      self.create_file_id(),
                                      headers=self._hdrs)

        self.assertEqual(self.srmock.status, falcon.HTTP_404)

        # invalid file id
        response = self.simulate_post(self._files_path + '/' +
                                      '(%^(*&^',
                                      headers=self._hdrs)

        self.assertEqual(self.srmock.status, falcon.HTTP_400)

        hdrs = {'content-type': 'application/x-deuce-block-list'}
        hdrs.update(self._hdrs)
        enough_num = int(conf.api_configuration.default_returned_num)

        # Register enough_num of blocks into system.
        block_list, blocks_data = self.helper_create_blocks(
            num_blocks=enough_num)

        # NOTE(TheSriram): data is list of lists of the form:
        # [[blockid, offset], [blockid, offset]]
        data = json.dumps([[block_list[cnt], cnt * 100]
                 for cnt in range(0, enough_num)])

        response = self.simulate_post(self._distractor_fileblocks_path,
                                      body=data, headers=hdrs)

        # Add blocks to FILES, resp has a list of missing blocks.
        response = self.simulate_post(self._fileblocks_path, body=data,
                                      headers=hdrs)

        self.assertGreater(len(response[0].decode()), 2)
        # assert len(response.body) > 2

        # Put the blocks to storage.
        self.helper_store_blocks(self.vault_id, blocks_data)

        # Add the same blocks to FILES again, resp is empty.

        response = self.simulate_post(self._fileblocks_path, body=data,
                                      headers=hdrs)

        self.assertEqual(len(response[0].decode()), 2)
        # assert len(response.body) == 2

        # Get unfinalized file.
        response = self.simulate_get(self._file_path, headers=hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_409)

        # Register 1.20 times of blocks into system.
        enough_num2 = int(1.2 * conf.api_configuration.default_returned_num)

        block_list2, blocks_data2 = self.helper_create_blocks(num_blocks=(
            enough_num2 - enough_num))

        # NOTE(TheSriram): data2 is list of lists of the form:
        # [[blockid, offset], [blockid, offset]]
        data2 = json.dumps([[block_list2[cnt - enough_num], cnt * 100]
                 for cnt in range(enough_num, enough_num2)])

        response = self.simulate_post(self._fileblocks_path, body=data2,
                                      headers=hdrs)
        self.assertGreater(len(response[0].decode()), 2)
        # assert len(response.body) > 2

        # Put the blocks to storage.
        self.helper_store_blocks(self.vault_id, blocks_data2)

        # Add blocks. resp will be empty.

        response = self.simulate_post(self._fileblocks_path, body=data2,
                                      headers=hdrs)
        self.assertEqual(len(response[0].decode()), 2)

        # Get the file.
        response = self.simulate_get(self._file_path, headers=hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_409)

        # Failed Finalize file for block gap & overlap

        failhdrs = hdrs.copy()
        failhdrs['x-file-length'] = '100'
        response = self.simulate_post(self._file_path,
                                      headers=failhdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_409)

        # Successfully finalize file
        good_hdrs = hdrs.copy()
        good_hdrs['x-file-length'] = str(enough_num2 * 100)
        response = self.simulate_post(self._file_path, headers=good_hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_200)

        # Error on trying to refinalize Finalized file.
        response = self.simulate_post(self._file_path, headers=hdrs)

        self.assertEqual(self.srmock.status, falcon.HTTP_409)

        # Error on trying to reassign blocks to finalized file
        response = self.simulate_post(self._fileblocks_path,
                                      body=data, headers=hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_409)
        # Get finalized file.
        response = self.simulate_get(self._file_path, headers=hdrs)
        actual_file = list(response)
        file_length = sum(len(file_chunk) for file_chunk in actual_file)
        self.assertEqual(file_length, enough_num2 * 100)
        self.assertEqual(self.srmock.status, falcon.HTTP_200)

        # List the blocks that make up this file
        self.helper_test_file_blocks_controller(self._file_path, hdrs)

        # Delete the finalized file. delete returns 'ok'
        response = self.simulate_delete(self._file_path, headers=hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_204)

    def test_check_x_ref_modified_with_change_in_references(self):
        hdrs = {'content-type': 'application/x-deuce-block-list'}
        hdrs.update(self._hdrs)
        enough_num = int(conf.api_configuration.default_returned_num)
        # Register enough_num of blocks into system.
        block_list, blocks_data = self.helper_create_blocks(
            num_blocks=enough_num)
        data = json.dumps([[block_list[cnt], cnt * 100]
                 for cnt in range(0, enough_num)])
        self.helper_store_blocks(self.vault_id, blocks_data)
        control_block = block_list[randrange(0, enough_num)]
        response = self.simulate_head(self.get_block_path(self.vault_id,
                                                          control_block),
                                      headers=hdrs)
        timestamp_upload_block = self.srmock.headers_dict['x-ref-modified']
        # NOTE(TheSriram): data is list of lists of the form:
        # [[blockid, offset], [blockid, offset]]

        time.sleep(1)

        # NOTE(TheSriram): assign blocks to file after sleeping for 1 second,
        # since granularity of 'x-ref-modified' is 1 second.
        # the reference count of the blocks should increase, thereby
        # modifying 'x-ref-modified' timestamp

        response = self.simulate_post(self._fileblocks_path,
                                      body=data, headers=hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_200)
        response = self.simulate_head(self.get_block_path(self.vault_id,
                                                          control_block),
                                      headers=hdrs)
        timestamp_assign_block = self.srmock.headers_dict['x-ref-modified']
        self.assertGreater(int(timestamp_assign_block),
                           int(timestamp_upload_block))
        time.sleep(1)

        # NOTE(TheSriram): delete file after sleeping for 1 second,
        # the reference count of the blocks should decrease, thereby
        # modifying 'x-ref-modified' timestamp

        response = self.simulate_delete(self._file_path,
                                        headers=hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_204)

        response = self.simulate_head(self.get_block_path(self.vault_id,
                                                          control_block),
                                      headers=hdrs)
        timestamp_delete_file = self.srmock.headers_dict['x-ref-modified']
        self.assertGreater(int(timestamp_delete_file),
                           int(timestamp_assign_block))

    def test_nonexistent_file_endpoints(self):
        file_path_format = '/v1.0/vaults/{0}/files/{1}'

        incorrect_vault = file_path_format.format('bogus_vault',
                                                  self.create_file_id())

        response = self.simulate_get(incorrect_vault, headers=self._hdrs)

        self.assertEqual(self.srmock.status, falcon.HTTP_404)

        incorrect_file = file_path_format.format(self.vault_id,
                                                 self.create_file_id())

        response = self.simulate_get(incorrect_file, headers=self._hdrs)

        self.assertEqual(self.srmock.status, falcon.HTTP_404)

        incorrect_file_get_blocks = file_path_format.format(self.vault_id,
                                            self.create_file_id()) + '/blocks'

        response = self.simulate_get(incorrect_file_get_blocks,
                                     headers=self._hdrs)

        self.assertEqual(self.srmock.status, falcon.HTTP_404)

    def test_non_existent_file_blocks_endpoints(self):
        file_blocks_path_format = '/v1.0/vaults/{0}/files/{1}/blocks'
        incorrect_vault = file_blocks_path_format.format('mock',
                                                  self.create_file_id())
        response = self.simulate_post(incorrect_vault,
                                      headers=self._hdrs,
                                      body="[['mockid','mockvalue']]")
        self.assertEqual(self.srmock.status, falcon.HTTP_400)

        incorrect_file = file_blocks_path_format.format(self.vault_id,
                                                        self.create_file_id())
        response = self.simulate_post(incorrect_file,
                                      headers=self._hdrs,
                                      body="[['mockid','mockvalue']]")
        self.assertEqual(self.srmock.status, falcon.HTTP_404)

    def helper_test_file_blocks_controller(self, file_id, hdrs):
        # Get blocks of a file.
        response = self.simulate_get(file_id + '/blocks', headers=hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_200)
        next_batch = self.srmock.headers_dict.get("X-Next-Batch")
        if next_batch:
            next_batch_url, query_string = relative_uri(next_batch)
        else:
            next_batch_url = next_batch

        # vault does not exists
        response = self.simulate_get(self._NOT_EXIST_files_path +
                                     '/%s/blocks' % (file_id,),
                                     headers=self._hdrs)

        self.assertEqual(self.srmock.status, falcon.HTTP_404)

        # fileid does not exists
        nonexistent_fileid = self.create_file_id()
        response = self.simulate_get(self._files_path +
                                     '/%s/blocks' % (nonexistent_fileid),
                                     headers=self._hdrs)

        self.assertEqual(self.srmock.status, falcon.HTTP_404)

        # Get blocks of a file. with limit not zero

        response = self.simulate_get(file_id + '/blocks',
                                     query_string='limit=5', headers=hdrs)

        self.assertEqual(self.srmock.status, falcon.HTTP_200)

        next_batch = self.srmock.headers_dict.get("X-Next-Batch")
        if next_batch:
            next_batch_url, query_string = relative_uri(next_batch)
        else:
            next_batch_url = next_batch

        resp_block_list = []
        params = {'marker': 0}
        while True:
            response = self.simulate_get(file_id + '/blocks',
                                         query_string=set_qs(file_id, params),
                                         headers=self._hdrs)

            next_batch = self.srmock.headers_dict.get("X-Next-Batch")
            if next_batch:
                next_batch_url, query_string = relative_uri(next_batch)
            else:
                next_batch_url = next_batch
            resp_block_list += json.loads(response[0].decode())
            assert isinstance(json.loads(response[0].decode()), list)
            if not next_batch_url:
                break
            # TODO (TheSriram): Make finding marker more elegant
            for query in query_string.split('&'):
                if 'marker' in query:
                    current_marker = query.split('marker=')[1]
            params['marker'] = current_marker

        self.assertEqual(len(resp_block_list), 1.2 *
                         conf.api_configuration.default_returned_num)

    def test_files_error(self):
        from deuce.model import Vault
        with patch.object(Vault, 'get', return_value=False):
            self.simulate_delete(self._file_path, headers=self._hdrs)
            self.assertEqual(self.srmock.status, falcon.HTTP_404)
        with patch.object(Vault, 'get_file', return_value=False):
            self.simulate_delete(self._file_path, headers=self._hdrs)
            self.assertEqual(self.srmock.status, falcon.HTTP_404)
            self.simulate_get(self._file_path + '/blocks', headers=self._hdrs)
            self.assertEqual(self.srmock.status, falcon.HTTP_404)
        self.simulate_delete(self._file_path, headers=self._hdrs)
        self.assertEqual(self.srmock.status, falcon.HTTP_204)

    def helper_get_files(self, marker, limit, assert_return_url,
                         assert_data_len, repeat=False):

        resp_file_list = []

        params = dict()

        if limit is not None:
            params['limit'] = limit

        while True:
            response = self.simulate_get(self._files_path,
                                         query_string=set_qs(
                                             self._files_path,
                                             params),
                                         headers=self._hdrs)

            next_batch = self.srmock.headers_dict.get("X-Next-Batch")
            if next_batch:
                next_batch_url, query_string = relative_uri(next_batch)
            else:
                next_batch_url = next_batch

            resp_file_list += json.loads(response[0].decode())
            assert isinstance(json.loads(response[0].decode()), list)

            if not repeat:
                assert (not next_batch_url) == (not assert_return_url)
                assert len(resp_file_list) == assert_data_len
                for h in resp_file_list:
                    assert h in self.file_list
                if assert_data_len == -1 or \
                        assert_data_len == self.total_file_num:
                    for h in self.file_list:
                        assert h in resp_file_list
                return next_batch_url
            if not next_batch_url:
                break

            # TODO (TheSriram): Make finding marker more elegant
            for query in query_string.split('&'):
                if 'marker' in query:
                    current_marker = query.split('marker=')[1]
            params['marker'] = current_marker

        self.assertEqual(len(resp_file_list), assert_data_len)

        for h in resp_file_list:
            assert h in self.file_list
        for h in self.file_list:
            assert h in resp_file_list
