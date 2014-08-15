import os
import hashlib
import uuid
from unittest import TestCase
from pecan import set_config
import pecan
from pecan.testing import load_test_app

__all__ = ['FunctionalTest']

import shutil


prod_conf = None
conf_dict = {}


def setUp():
    """
        Unit tests environment setup.
        Called only once at the beginning.
    """
    global prod_conf
    global conf_dict
    if not os.path.exists('/tmp/block_storage'):
        os.mkdir('/tmp/block_storage')

    # Copy the production configs to the test path.
    if not os.path.exists('tests/public'):
        os.symlink('../../public', 'tests/public')
    if not os.path.exists('tests/deuce/templates'):
        if not os.path.exists('tests/deuce'):
            os.mkdir('tests/deuce')
        os.symlink('../../../deuce/templates', 'tests/deuce/templates')

    # Cook config.py for unit tests.
    prod_conf = pecan.configuration.conf_from_file('../config.py')
    conf_dict = prod_conf.to_dict()

    # To update existed items.
    # MongoDB
    conf_dict['metadata_driver']['mongodb']['db_module'] = \
        'deuce.tests.db_mocking.mongodb_mocking'
    conf_dict['metadata_driver']['mongodb']['FileBlockReadSegNum'] = 10
    conf_dict['metadata_driver']['mongodb']['maxFileBlockSegNum'] = 30
    # Cassandra
    conf_dict['metadata_driver']['cassandra']['db_module'] = \
        'deuce.tests.mock_cassandra'
    conf_dict['metadata_driver']['cassandra']['is_mocking'] = True
    # Swift
    conf_dict['block_storage_driver']['swift']['swift_module'] = \
        'deuce.tests.db_mocking.swift_mocking'
    conf_dict['block_storage_driver']['swift']['is_mocking'] = True
    # conf_dict['block_storage_driver']['swift']['swift_module'] = \
    #     'swiftclient'
    # conf_dict['block_storage_driver']['swift']['is_mocking'] = False

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

    # Cleanup the test configs.
    if os.path.exists('tests/public'):
        os.remove('tests/public')
    if os.path.exists('tests/deuce/templates'):
        shutil.rmtree('tests/deuce')


class FunctionalTest(TestCase):
    """
    Used for functional tests where you need to test your
    literal application and its integration with the framework.
    """
    def setUp(self):
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
        class DummyObject(object):
            pass

        state = DummyObject()
        state.response = DummyObject()
        state.request = DummyObject()
        state.request.headers = headers

        # initialize all hooks with the 'state' object from above
        for hook in prod_conf.get_hooks():
            hook.on_route(state)
