from abc import ABCMeta, abstractmethod
import hashlib
import os
import shutil
from unittest import TestCase
import uuid

from pecan import set_config
import pecan
from pecan.testing import load_test_app
import six
from six.moves.urllib.parse import urlparse, parse_qs

__all__ = ['FunctionalTest', 'DriverTest']

prod_conf = None
conf_dict = {}


class DummyContextObject(object):
    pass


def setUp():
    """
        Unit tests environment setup.
        Called only once at the beginning.
    """
    global prod_conf
    global conf_dict
    if not os.path.exists('/tmp/block_storage'):
        os.mkdir('/tmp/block_storage')

    # Cook config.py for unit tests.
    prod_conf = pecan.configuration.conf_from_file('../config.py')
    conf_dict = prod_conf.to_dict()

    import logging
    LOG = logging.getLogger(__name__)

    # To update existed items.
    # MongoDB
    LOG.info('MongoDB - Mocking: {0:}'.format(
        conf_dict['metadata_driver']['mongodb']['testing']['is_mocking']))
    if conf_dict['metadata_driver']['mongodb']['testing']['is_mocking']:
        conf_dict['metadata_driver']['mongodb']['db_module'] = \
            'deuce.tests.db_mocking.mongodb_mocking'
        conf_dict['metadata_driver']['mongodb']['FileBlockReadSegNum'] = 10
        conf_dict['metadata_driver']['mongodb']['maxFileBlockSegNum'] = 30

    # Cassandra
    LOG.info('Cassandra - Mocking: {0:}'.format(
        conf_dict['metadata_driver']['cassandra']['testing']['is_mocking']))
    if conf_dict['metadata_driver']['cassandra']['testing']['is_mocking']:
        conf_dict['metadata_driver']['cassandra']['db_module'] = \
            'deuce.tests.mock_cassandra'

    # Swift
    LOG.info('Swift - Mocking: {0:}'.format(
        conf_dict['block_storage_driver']['swift']['testing']['is_mocking']))
    if conf_dict['block_storage_driver']['swift']['testing']['is_mocking']:
        conf_dict['block_storage_driver']['swift']['swift_module'] = \
            'deuce.tests.db_mocking.swift_mocking'

    # To add for-test-only items.
    # conf_dict['metadata_driver']['mongodb']['foo'] = 'bar'


def tearDown():
    """
        Unit tests environment cleanup.
        Called only once at the end.
    """
    shutil.rmtree('/tmp/block_storage')

    # Always remove the database so that we can start over on
    # test execution
    # Drop sqlite DB
    if os.path.exists('/tmp/deuce_sqlite_unittest_vaultmeta.db'):
        os.remove('/tmp/deuce_sqlite_unittest_vaultmeta.db')


class FunctionalTest(TestCase):

    """
    Used for functional tests where you need to test your
    literal application and its integration with the framework.
    """

    def setUp(self):
        import deuce
        deuce.context = DummyContextObject
        deuce.context.project_id = self.create_project_id()
        deuce.context.openstack = DummyContextObject()
        deuce.context.openstack.auth_token = self.create_auth_token()
        deuce.context.openstack.swift = DummyContextObject()
        deuce.context.openstack.swift.storage_url = 'storage.url'

        global conf_dict
        self.app = load_test_app(config=conf_dict)

    def tearDown(self):
        set_config({}, overwrite=True)

        import deuce
        deuce.context = None

    def create_auth_token(self):
        """Create a dummy Auth Token."""
        return 'auth_{0:}'.format(str(uuid.uuid4()))

    def create_project_id(self):
        """Create a dummy project ID. This could be
        anything, but for ease-of-use we just make it
        a uuid"""
        return 'project_{0:}'.format(str(uuid.uuid4()))

    def create_block_id(self, data=None):
        sha1 = hashlib.sha1()
        sha1.update(data or os.urandom(2048))
        return sha1.hexdigest()

    def create_vault_id(self):
        """Creates a dummy vault ID. This could be
        anything, but for ease-of-use we just make it
        a uuid"""
        return 'vault_{0:}'.format(str(uuid.uuid4()))

    def create_file_id(self):
        return str(uuid.uuid4())

    def calc_sha1(self, data):
        sha1 = hashlib.sha1()
        sha1.update(data)
        return sha1.hexdigest()


@six.add_metaclass(ABCMeta)
class DriverTest(FunctionalTest):
    """
    Used for testing Deuce Drivers
    """

    def setUp(self):
        super(DriverTest, self).setUp()

    def tearDown(self):
        super(DriverTest, self).tearDown()

    @abstractmethod
    def create_driver(self):
        raise NotImplementedError()


@six.add_metaclass(ABCMeta)
class ControllerTest(FunctionalTest):
    """
    Used for testing Deuce Controllers
    """

    def setUp(self):
        super(ControllerTest, self).setUp()

    def tearDown(self):
        super(ControllerTest, self).tearDown()

    def get_block_path(self, blockid):
        return '{0}/{1}'.format(self._blocks_path, blockid)

    def helper_create_vault(self, vault_name, hdrs):
        vault_path = '/v1.0/vaults/{0}'.format(vault_name)
        response = self.app.put(vault_path, headers=hdrs)

    def helper_delete_vault(self, vault_name, hdrs):
        vault_path = '/v1.0/vaults/{0}'.format(vault_name)
        response = self.app.delete(vault_path, headers=hdrs)

    def helper_create_files(self, num):
        params = {}
        hdrs = self._hdrs.copy()
        hdrs['x-file-length'] = '0'
        for cnt in range(0, num):
            response = self.app.post(self._files_path, headers=self._hdrs)
            file_id = response.headers["Location"]
            response = self.app.post(file_id,
                                     params=params, headers=hdrs)
            file_id = urlparse(file_id).path.split('/')[-1]
            self.file_list.append(file_id)
        return num

    def helper_create_blocks(self, num_blocks):

        block_sizes = [100 for x in
            range(0, num_blocks)]

        data = [os.urandom(x) for x in block_sizes]
        block_list = [self.calc_sha1(d) for d in data]

        block_data = zip(block_sizes, data, block_list)

        return block_list, block_data

    def helper_store_blocks(self, block_data):

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


@six.add_metaclass(ABCMeta)
class HookTest(FunctionalTest):
    """
    Used for testing Deuce Controllers
    """

    def setUp(self):
        super(HookTest, self).setUp()

    def tearDown(self):
        super(HookTest, self).tearDown()

    @abstractmethod
    def create_hook(self):
        raise NotImplementedError()

    def create_service_catalog(self, objectStoreType='object-store',
                               endpoints=True, region='test',
                               url='url-data'):
        catalog = {
            'access': {
                'serviceCatalog': []
            }
        }

        if len(objectStoreType):
            service = {
                'name': 'test-service',
                'type': objectStoreType,
                'endpoints': [
                ]
            }
            if endpoints:
                endpoint = {
                    'internalURL': url,
                    'publicURL': url,
                    'tenantId': '9876543210',
                    'region': region,
                }
                service['endpoints'].append(endpoint)
            catalog['access']['serviceCatalog'].append(service)

        return catalog
