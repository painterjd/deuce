from cafe.drivers.unittest import fixtures
from utils import config
from utils import client
from utils.schema import auth

from collections import namedtuple

import json
import jsonschema
import msgpack
import os
import random
import re
import sha
import string
import urlparse

Block = namedtuple('Block', 'Id Data')
File = namedtuple('File', 'Id Url')


class TestBase(fixtures.BaseTestFixture):
    """
    Fixture for Deuce API Tests
    """

    @classmethod
    def setUpClass(cls):
        """
        Initialization of Deuce Client
        """

        super(TestBase, cls).setUpClass()
        cls.config = config.deuceConfig()
        cls.auth_config = config.authConfig()
        cls.auth_token = None
        cls.storage_config = config.storageConfig()
        cls.storage_url = ''
        cls.tenantid = None
        cls.region = cls.storage_config.region
        if cls.config.use_auth:
            cls.a_client = client.AuthClient(cls.auth_config.base_url)
            cls.a_resp = cls.a_client.get_auth_token(cls.auth_config.user_name,
                                                     cls.auth_config.api_key)
            jsonschema.validate(cls.a_resp.json(), auth.authentication)
            cls.auth_token = cls.a_resp.entity.token

            cls.tenantid = cls.a_resp.entity.regions[cls.region]['tenantId']
            url_type = 'internalURL' if cls.storage_config.internal_url \
                else 'publicURL'
            cls.storage_url = cls.a_resp.entity.regions[cls.region][url_type]
        cls.client = client.BaseDeuceClient(cls.config.base_url,
                                            cls.config.version,
                                            cls.auth_token,
                                            cls.storage_url,
                                            cls.tenantid)

        cls.vaults = []
        cls.blocks = []
        cls.api_version = cls.config.version

    @classmethod
    def tearDownClass(cls):
        """
        Deletes the added resources
        """
        super(TestBase, cls).tearDownClass()

    @classmethod
    def id_generator(cls, size):
        """
        Return an alphanumeric string of size
        """

        return ''.join(random.choice(string.ascii_letters +
            string.digits + '-_') for _ in range(size))

    def setUp(self):
        super(TestBase, self).setUp()

    def tearDown(self):
        if any(r for r in self._resultForDoCleanups.failures
               if self._custom_test_name_matches_result(self._testMethodName,
                                                        r)):
            self._reporter.stop_test_metrics(self._testMethodName, 'Failed')
        elif any(r for r in self._resultForDoCleanups.errors
                 if self._custom_test_name_matches_result(self._testMethodName,
                                                          r)):
            self._reporter.stop_test_metrics(self._testMethodName, 'ERRORED')
        else:
            super(TestBase, self).tearDown()

    def _custom_test_name_matches_result(self, name, test_result):
        """
        Function used to compare test name with the information in test_result
        Used with Nosetests
        """

        try:
            result = test_result[0]
            testMethodName = result.__str__().split()[0]
        except:
            return False
        return testMethodName == name

    def assertHeaders(self, headers, json=False, binary=False):
        """Basic http header validation"""

        self.assertIsNotNone(headers['transaction-id'])
        self.assertIsNotNone(headers['content-length'])
        if json:
            self.assertEqual(headers['content-type'],
                             'application/json; charset=UTF-8')
        if binary:
            content_type = headers['content-type'].split(';')[0]
            self.assertEqual(content_type,
                             'application/octet-stream')

    def assertUrl(self, url, base=False, vaults=False, vaultspath=True,
                  blocks=False, files=False, filepath=False, fileblock=False,
                  nextlist=False):

        msg = 'url: {0}'.format(url)
        u = urlparse.urlparse(url)
        self.assertIn(u.scheme, ['http', 'https'])

        if base:
            self.assertEqual(u.path, '/{0}'.format(self.api_version), msg)

        if vaults:
            self.assertEqual(u.path, '/{0}/vaults/'.format(self.api_version,
                                                          msg))

        if vaultspath:
            valid = re.compile('/{0}/vaults/[a-zA-Z0-9\-_]*'
                               ''.format(self.api_version))
            self.assertIsNotNone(valid.match(u.path), msg)

        if blocks:
            self.assertEqual(u.path, '/{0}/vaults/{1}/blocks'
                             ''.format(self.api_version, self.vaultname), msg)

        if files:
            self.assertEqual(u.path, '/{0}/vaults/{1}/files'
                             ''.format(self.api_version, self.vaultname), msg)

        if filepath:
            valid = re.compile('/{0}/vaults/{1}/files/[a-zA-Z0-9\-_]*'
                               ''.format(self.api_version, self.vaultname))
            self.assertIsNotNone(valid.match(u.path), msg)

        if fileblock:
            valid = re.compile('/{0}/vaults/{1}/files/[a-zA-Z0-9\-_]*/blocks'
                               ''.format(self.api_version, self.vaultname))
            self.assertIsNotNone(valid.match(u.path), msg)

        if nextlist:
            query = urlparse.parse_qs(u.query)
            self.assertIn('marker', query, msg)
            self.assertIn('limit', query, msg)

    def _create_empty_vault(self, vaultname=None, size=50):
        """
        Test Setup Helper: Creates an empty vault
        If vaultname is provided, the vault is created using that name.
        If not, an alphanumeric vaultname of a given size is generated
        """

        if vaultname:
            self.vaultname = vaultname
        else:
            self.vaultname = self.id_generator(size)
        resp = self.client.create_vault(self.vaultname)
        return 201 == resp.status_code

    def create_empty_vault(self, vaultname=None, size=50):
        """
        Test Setup Helper: Creates an empty vault
        If vaultname is provided, the vault is created using that name.
        If not, an alphanumeric vaultname of a given size is generated

        Exception is raised if the operation is not successful
        """
        if not self._create_empty_vault(vaultname, size):
            raise Exception('Failed to create vault')
        self.vaults.append(self.vaultname)
        self.blocks = []
        self.files = []

    def generate_block_data(self, block_data=None, size=30720):
        """
        Test Setup Helper: Generates block data and adds it to the internal
        block list
        """

        if block_data is not None:
            self.block_data = block_data
        else:
            self.block_data = os.urandom(size)
        self.blockid = sha.new(self.block_data).hexdigest()
        self.blocks.append(Block(Id=self.blockid, Data=self.block_data))

    def _upload_block(self, block_data=None, size=30720):
        """
        Test Setup Helper: Uploads a block
        If block_data is used if provided.
        If not, a random block of data of the specified size is used
        """
        self.generate_block_data(block_data, size)
        resp = self.client.upload_block(self.vaultname, self.blockid,
                                        self.block_data)
        return 201 == resp.status_code

    def upload_block(self, block_data=None, size=30720):
        """
        Test Setup Helper: Uploads a block
        If block_data is used if provided.
        If not, a random block of data of the specified size is used

        Exception is raised if the operation is not successful
        """
        if not self._upload_block(block_data, size):
            raise Exception('Failed to upload block')

    def _upload_multiple_blocks(self, nblocks, size=30720):
        """
        Test Setup Helper: Uploads multiple blocks using msgpack
        """
        prev_blocks = self.blocks[:]
        [self.generate_block_data(size=size) for _ in range(nblocks)]
        # uploaded new generated blocks
        uploaded = list(set(self.blocks) - set(prev_blocks))
        data = dict([(block.Id, block.Data) for block in uploaded])
        msgpack_data = msgpack.packb(data)
        resp = self.client.upload_multiple_blocks(self.vaultname, msgpack_data)
        return 201 == resp.status_code

    def upload_multiple_blocks(self, nblocks, size=30720):
        """
        Test Setup Helper: Uploads multiple blocks using msgpack

        Exception is raised if the operation is not successful
        """
        if not self._upload_multiple_blocks(nblocks, size):
            raise Exception('Failed to upload multiple blocks')

    def _create_new_file(self):
        """
        Test Setup Helper: Creates a file
        """

        resp = self.client.create_file(self.vaultname)
        self.fileurl = resp.headers['location']
        self.fileid = self.fileurl.split('/')[-1]
        self.files.append(File(Id=self.fileid, Url=self.fileurl))
        return 201 == resp.status_code

    def create_new_file(self):
        """
        Test Setup Helper: Creates a file

        Exception is raised if the operation is not successful
        """

        if not self._create_new_file():
            raise Exception('Failed to create a file')

    def assign_all_blocks_to_file(self, offset_divisor=None):
        """
        Test Setup Helper: Assigns all blocks to the file

        Exception is raised if the operation is not successful
        """

        if not self._assign_blocks_to_file(offset_divisor=offset_divisor):
            raise Exception('Failed to assign blocks to file')

    def _assign_blocks_to_file(self, offset=0, blocks=[],
                              offset_divisor=None, file_url=None):
        """
        Test Setup Helper: Assigns blocks to the file
        """

        block_list = list()
        if len(blocks) == 0:
            blocks = range(len(self.blocks))
        if not file_url:
            file_url = self.fileurl

        for index in blocks:
            block_info = self.blocks[index]
            block_list.append({'id': block_info.Id,
                               'size': len(block_info.Data), 'offset': offset})
            if offset_divisor:
                offset += len(block_info.Data) / offset_divisor
            else:
                offset += len(block_info.Data)

        block_dict = {'blocks': block_list}
        resp = self.client.assign_to_file(json.dumps(block_dict),
                                          alternate_url=file_url)
        return 200 == resp.status_code

    def assign_blocks_to_file(self, offset=0, blocks=[],
                              offset_divisor=None, file_url=None):
        """
        Test Setup Helper: Assigns blocks to the file

        Exception is raised if the operation is not successful
        """

        if not self._assign_blocks_to_file(offset, blocks, offset_divisor,
                                           file_url):
            raise Exception('Failed to assign blocks to file')

    def _finalize_file(self, file_url=None):
        """
        Test Setup Helper: Finalizes the file
        """

        if not file_url:
            file_url = self.fileurl
        resp = self.client.finalize_file(alternate_url=file_url)
        return 200 == resp.status_code

    def finalize_file(self, file_url=None):
        """
        Test Setup Helper: Finalizes the file

        Exception is raised if the operation is not successful
        """

        if not self._finalize_file(file_url):
            raise Exception('Failed to finalize file')
