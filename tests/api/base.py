from cafe.drivers.unittest import fixtures
from utils import config
from utils import client
from utils.schema import auth

from collections import namedtuple

import json
import jsonschema
import os
import random
import re
import sha
import string
import urlparse

Block = namedtuple('Block', 'Id Data')


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
        cls.storage_url = None
        if cls.config.use_auth:
            cls.a_client = client.AuthClient(cls.auth_config.base_url)
            cls.a_resp = cls.a_client.get_auth_token(cls.auth_config.user_name,
                                                     cls.auth_config.api_key)
            jsonschema.validate(cls.a_resp.json(), auth.authentication)
            cls.auth_token = cls.a_resp.entity.token
        if cls.config.use_storage:
            cls.storage_url = cls.storage_config.base_url
        cls.client = client.BaseDeuceClient(cls.config.base_url,
                                            cls.config.version,
                                            cls.auth_token,
                                            cls.storage_url)

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

    def assertUrl(self, url, base=False, blockpath=False, files=False,
                  filepath=False, fileblock=False, nextlist=False):

        msg = 'url: {0}'.format(url)
        u = urlparse.urlparse(url)
        self.assertIn(u.scheme, ['http', 'https'])

        if base:
            self.assertEqual(u.path, '/{0}'.format(self.api_version), msg)

        if blockpath:
            self.assertEqual(u.path, '/{0}/{1}/blocks'
                             ''.format(self.api_version, self.vaultname), msg)

        if files:
            self.assertEqual(u.path, '/{0}/{1}/files'
                             ''.format(self.api_version, self.vaultname), msg)

        if filepath:
            valid = re.compile('/{0}/{1}/files/[a-zA-Z0-9\-_]*'
                               ''.format(self.api_version, self.vaultname))
            self.assertIsNotNone(valid.match(u.path), msg)

        if fileblock:
            valid = re.compile('/{0}/{1}/files/[a-zA-Z0-9\-_]*/blocks'
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

    def _create_new_file(self):
        """
        Test Setup Helper: Creates a file
        """

        resp = self.client.create_file(self.vaultname)
        self.fileurl = resp.headers['location']
        self.fileid = self.fileurl.split('/')[-1]
        self.files.append(self.fileid)
        return 201 == resp.status_code

    def create_new_file(self):
        """
        Test Setup Helper: Creates a file

        Exception is raised if the operation is not successful
        """

        if not self._create_new_file():
            raise Exception('Failed to create a file')

    def _assign_all_blocks_to_file(self, offset_divisor=None):
        """
        Test Setup Helper: Assigns all blocks to the file
        """
        offset = 0
        block_list = list()
        for block_info in self.blocks:
            block_list.append({'id': block_info.Id,
                               'size': len(block_info.Data), 'offset': offset})
            if offset_divisor:
                offset += len(block_info.Data) / offset_divisor
            else:
                offset += len(block_info.Data)
        block_dict = {'blocks': block_list}
        resp = self.client.assign_to_file(json.dumps(block_dict),
                                          alternate_url=self.fileurl)
        return 200 == resp.status_code

    def assign_all_blocks_to_file(self, offset_divisor=None):
        """
        Test Setup Helper: Assigns all blocks to the file

        Exception is raised if the operation is not successful
        """

        if not self._assign_all_blocks_to_file(offset_divisor):
            raise Exception('Failed to assign blocks to file')

    def _finalize_file(self):
        """
        Test Setup Helper: Finalizes the file
        """

        resp = self.client.finalize_file(alternate_url=self.fileurl)
        return 200 == resp.status_code

    def finalize_file(self):
        """
        Test Setup Helper: Finalizes the file

        Exception is raised if the operation is not successful
        """

        if not self._finalize_file():
            raise Exception('Failed to finalize file')
