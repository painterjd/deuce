from abc import ABCMeta, abstractmethod
import unittest
from falcon import testing as ftest
import deuce
from deuce.transport.wsgi.driver import Driver
import deuce.util.log as logging
import os
import hashlib
import uuid
import six
from six.moves.urllib.parse import urlparse, parse_qs


__all__ = ['V1Base']

import shutil


class DummyContextObject(object):
    pass


def setUp():
    """
        Unit tests environment setup.
        Called only once at the beginning.
    """
    if not os.path.exists('/tmp/block_storage'):
        os.mkdir('/tmp/block_storage')

    logging.setup()


def tearDown():
    """
        Unit tests environment cleanup.
        Called only once at the end.
    """
    deuce.conf = None
    shutil.rmtree('/tmp/block_storage')

    # Always remove the database so that we can start over on
    # test execution
    # Drop sqlite DB
    if os.path.exists('/tmp/deuce_sqlite_unittest_vaultmeta.db'):
        os.remove('/tmp/deuce_sqlite_unittest_vaultmeta.db')


class TestBase(unittest.TestCase):

    def setUp(self):
        super(TestBase, self).setUp()
        import deuce
        deuce.context = DummyContextObject
        deuce.context.project_id = self.create_project_id()
        deuce.context.openstack = DummyContextObject()
        deuce.context.openstack.auth_token = self.create_auth_token()
        deuce.context.openstack.swift = DummyContextObject()
        deuce.context.openstack.swift.storage_url = 'storage.url'
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
    pass


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
        params = {}
        hdrs = self._hdrs.copy()
        hdrs['x-file-length'] = '0'
        for cnt in range(0, num):
            response = self.simulate_post(self._files_path, headers=self._hdrs)
            file_id = self.srmock.headers_dict['Location']
            response = self.simulate_post(file_id,
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

            response = self.simulate_put(path, headers=headers,
                                         body=data)
