from abc import ABCMeta, abstractmethod
import hashlib
import os
import shutil
import unittest
import uuid

import falcon
from falcon import testing as ftest
import six
from six.moves.urllib.parse import urlparse, parse_qs

import deuce
from deuce.transport.wsgi import v1_0
from deuce.transport.wsgi.driver import Driver
import deuce.util.log as logging
from deuce.util.misc import relative_uri

__all__ = ['V1Base']


class DummyContextObject(object):
    pass


test_disk_storage_location = '/tmp/block_storage_{0}'.format(str(uuid.uuid4()))
test_mongodb_location = '/tmp/deuce_mongo_unittest_vaultmeta_{0}.db'.format(
    str(uuid.uuid4()))


def setUp():
    """
        Unit tests environment setup.
        Called only once at the beginning.
    """
    if not os.path.exists(test_disk_storage_location):
        os.mkdir(test_disk_storage_location)

    logging.setup()


def tearDown():
    """
        Unit tests environment cleanup.
        Called only once at the end.
    """
    deuce.conf = None

    shutil.rmtree(test_disk_storage_location)


class TestBase(unittest.TestCase):

    def setUp(self):
        super(TestBase, self).setUp()

        import deuce
        deuce.context = DummyContextObject()
        deuce.context.project_id = self.create_project_id()
        deuce.context.openstack = DummyContextObject()
        deuce.context.openstack.auth_token = self.create_auth_token()
        deuce.context.openstack.swift = DummyContextObject()
        deuce.context.openstack.swift.storage_url = 'storage.url'

        # Override the storage locations
        # This is required for environments that run multiple tests
        # at the same time, e.g. tox -e py34 in one shell and
        # tox -e py33 in another shell simultaneously, f.e Jenkins
        deuce.conf.block_storage_driver.options.path = \
            test_disk_storage_location

        self.app = Driver().app
        self.srmock = ftest.StartResponseMock()
        self.headers = {}

    def tearDown(self):
        super(TestBase, self).tearDown()
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

    def create_storage_block_id(self):
        return '{0:}'.format(str(uuid.uuid4()))

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

    def simulate_request(self, path, **kwargs):
        """Simulate a request.

        Simulates a WSGI request to the API for testing.

        :param path: Request path for the desired resource
        :param kwargs: Same as falcon.testing.create_environ()

        :returns: standard WSGI iterable response
        """

        headers = kwargs.get('headers', self.headers).copy()
        kwargs['headers'] = headers
        return self.app(ftest.create_environ(path=path,
                                             protocol='HTTP/1.0',
                                             **kwargs),
                        self.srmock)

    def simulate_get(self, *args, **kwargs):
        """Simulate a GET request."""
        kwargs['method'] = 'GET'
        return self.simulate_request(*args, **kwargs)

    def simulate_head(self, *args, **kwargs):
        """Simulate a HEAD request."""
        kwargs['method'] = 'HEAD'
        return self.simulate_request(*args, **kwargs)

    def simulate_put(self, *args, **kwargs):
        """Simulate a PUT request."""
        kwargs['method'] = 'PUT'
        return self.simulate_request(*args, **kwargs)

    def simulate_post(self, *args, **kwargs):
        """Simulate a POST request."""
        kwargs['method'] = 'POST'
        return self.simulate_request(*args, **kwargs)

    def simulate_delete(self, *args, **kwargs):
        """Simulate a DELETE request."""
        kwargs['method'] = 'DELETE'
        return self.simulate_request(*args, **kwargs)

    def simulate_patch(self, *args, **kwargs):
        """Simulate a PATCH request."""
        kwargs['method'] = 'PATCH'
        return self.simulate_request(*args, **kwargs)


class V1Base(TestBase):

    """Base class for V1 API Tests.

    Should contain methods specific to V1 of the API
    """

    def setUp(self):
        super(V1Base, self).setUp()
        # Override the storage locations
        # This is required for environments that run multiple tests
        # at the same time, e.g. tox -e py34 in one shell and
        # tox -e py33 in another shell simultaneously, f.e Jenkins
        deuce.conf.metadata_driver.mongodb.db_file = test_mongodb_location

        if deuce.conf.metadata_driver.mongodb.testing.is_mocking:
            deuce.conf.metadata_driver.mongodb.db_module = \
                'deuce.tests.db_mocking.mongodb_mocking'
            deuce.conf.metadata_driver.mongodb.FileBlockReadSegNum = 10
            deuce.conf.metadata_driver.mongodb.maxFileBlockSegNum = 30

        if deuce.conf.metadata_driver.cassandra.testing.is_mocking:
            deuce.conf.metadata_driver.cassandra.db_module = \
                'deuce.tests.mock_cassandra'

        if deuce.conf.block_storage_driver.swift.testing.is_mocking:
            deuce.conf.block_storage_driver.swift.swift_module = \
                'deuce.tests.db_mocking.swift_mocking'


@six.add_metaclass(ABCMeta)
class HookTest(V1Base):

    """
    Used for testing Deuce Hooks
    """

    def app_setup(self, hooks):
        endpoints = [
            ('/v1.0', v1_0.public_endpoints()),
        ]
        self.app = falcon.API(before=hooks)
        for version_path, endpoints in endpoints:
            for route, resource in endpoints:
                self.app.add_route(version_path + route, resource)
        self.srmock = ftest.StartResponseMock()
        self.headers = {}

    def setUp(self):
        super(HookTest, self).setUp()

    def tearDown(self):
        super(HookTest, self).tearDown()

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


@six.add_metaclass(ABCMeta)
class DriverTest(V1Base):

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
class ControllerTest(V1Base):

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
        response = self.simulate_put(vault_path, headers=hdrs)

    def helper_delete_vault(self, vault_name, hdrs):
        vault_path = '/v1.0/vaults/{0}'.format(vault_name)
        response = self.simulate_delete(vault_path, headers=hdrs)

    def helper_create_files(self, num):
        hdrs = self._hdrs.copy()
        hdrs['x-file-length'] = '0'
        for cnt in range(0, num):
            response = self.simulate_post(self._files_path, headers=self._hdrs)
            file_path = self.srmock.headers_dict['Location']
            file_uri, querystring = relative_uri(file_path)
            response = self.simulate_post(file_uri, headers=hdrs)
            file_id = urlparse(file_path).path.split('/')[-1]
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

            headers = {
                "Content-Type": "application/octet-stream",
                "Content-Length": str(size),
            }

            headers.update(self._hdrs)

            response = self.simulate_put(path, headers=headers,
                                         body=data)
