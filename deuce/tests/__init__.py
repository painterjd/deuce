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

    def init_context(self, headers):
        """Create a Deuce Context based on the headers
        using the hooks; required for testing the controllers"""

        state = DummyContextObject()
        state.response = DummyContextObject()
        state.response.headers = {}
        state.request = DummyContextObject()
        state.request.headers = headers

        # initialize all hooks with the 'state' object from above
        for hook in prod_conf.get_hooks():
            hook.on_route(state)


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
