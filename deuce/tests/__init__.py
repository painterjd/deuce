import unittest
from falcon import testing as ftest
import deuce
from deuce.transport.wsgi.driver import Driver
import deuce.util.log as logging
import os
import hashlib
import uuid

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

    def simulate_request(self, path, **kwargs):
        """Simulate a request.

        Simulates a WSGI request to the API for testing.

        :param path: Request path for the desired resource
        :param kwargs: Same as falcon.testing.create_environ()

        :returns: standard WSGI iterable response
        """

        headers = kwargs.get('headers', self.headers).copy()
        kwargs['headers'] = headers
        return self.app(ftest.create_environ(path=path, **kwargs),
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
