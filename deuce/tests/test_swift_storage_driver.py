import six
from deuce.drivers.blockstoragedriver import BlockStorageDriver
from pecan import conf

from deuce.drivers.swift import SwiftStorageDriver

from deuce.tests.test_disk_storage_driver import DiskStorageDriverTest

# Users need take care of authenticate themselves and
# have the token ready for each query.

from swiftclient import client as Conn
from swiftclient.exceptions import ClientException


import sys
import os
import json

# TODO: Make this test generic -- it should not konw
# which particular driver it is testing.


class SwiftStorageDriverTest(DiskStorageDriverTest):
    # TODO (TheSriram) : Make pecan.conf swift version aware
    def setUp(self):
        super(SwiftStorageDriverTest, self).setUp()
        storage_url, auth_token = self.get_Auth_Token()
        from deuce.tests import DummyContextObject
        import deuce
        deuce.context.openstack = DummyContextObject()
        deuce.context.openstack.auth_token = auth_token
        deuce.context.openstack.swift = DummyContextObject()
        deuce.context.openstack.swift.storage_url = storage_url

    def get_Auth_Token(self):
        self.mocking = False
        try:
            if conf.block_storage_driver.swift.testing.is_mocking:
                self.mocking = True
        except:
            self.mocking = False

        if not self.mocking:
            auth_url = str(conf.block_storage_driver.swift.testing.auth_url)

            username = str(conf.block_storage_driver.swift.testing.username)
            password = str(conf.block_storage_driver.swift.testing.password)
            try:
                os_options = dict()
                storage_url, token = \
                    Conn.get_keystoneclient_2_0(
                        auth_url=auth_url,
                        user=username,
                        key=password,
                        os_options=os_options)
            except ClientException as e:
                sys.exit(str(e))

        else:
            storage_url = \
                str(conf.block_storage_driver.swift.testing.storage_url)
            token = 'mocking_token'

        self._hdrs = {"x-project-id": self.create_project_id(),
                      "x-auth-token": self.create_auth_token()}
        return storage_url, token

    def test_basic_construction(self):
        driver = SwiftStorageDriver()

    def create_driver(self):
        return SwiftStorageDriver()

    def test_ancestry(self):
        driver = SwiftStorageDriver()
        assert isinstance(driver, SwiftStorageDriver)
        assert isinstance(driver, object)

        # Test all exceptions
        driver = SwiftStorageDriver()

        import deuce
        valid_token = deuce.context.openstack.auth_token
        failed_token = valid_token + '1'
        deuce.context.openstack.auth_token = failed_token

        vaultid = 'notmatter'
        blockid = 'notmatter'
        block_ids = ['notmatter1', 'notmatter2']
        blockdatas = [b'data1', b'data2']
        driver.create_vault(vaultid)
        driver.vault_exists(vaultid)
        driver.store_block(vaultid, blockid,
                           str('').encode('utf-8'))
        driver.block_exists(vaultid, blockid)
        driver.get_block_obj(vaultid, blockid)
        driver.store_async_block(
            vaultid,
            block_ids,
            blockdatas)
        driver.delete_block(vaultid, blockid)
        for block_id in block_ids:
            driver.delete_block(vaultid, block_id)

        driver.delete_vault(vaultid)
        deuce.context.openstack.auth_token = valid_token

    def test_network_drops(self):
        """
        This is only to exercise code that relies on network errors to occur
        """
        self.mocking = False
        try:
            if conf.block_storage_driver.swift.testing.is_mocking:
                self.mocking = True
        except:
            self.mocking = False

        if self.mocking:

            vault_id = 'notmatter'
            block_id = 'notmatter'

            driver = self.create_driver()
            assert isinstance(driver, SwiftStorageDriver)
            assert isinstance(driver, object)

            # simulate swiftclient tossing exceptions
            driver.Conn.mock_drop_connections(True)

            self.assertFalse(driver.create_vault(vault_id))

            self.assertFalse(driver.vault_exists(vault_id))

            self.assertFalse(driver.delete_vault(vault_id))

            self.assertFalse(driver.store_block(vault_id, block_id,
                                                str('').encode('utf-8')))

            self.assertFalse(driver.store_async_block(
                             vault_id,
                             [block_id],
                             [str('').encode('utf-8')]))

            self.assertFalse(driver.block_exists(vault_id, block_id))

            self.assertFalse(driver.delete_block(vault_id, block_id))

            self.assertIsNone(driver.get_block_obj(vault_id, block_id))

            # Stats should come back as zero even though the connection
            # "dropped"
            bad_vault_stats = driver.get_vault_statistics(vault_id)
            main_keys = ('total-size', 'block-count')
            for key in main_keys:
                assert key in bad_vault_stats.keys()
                assert bad_vault_stats[key] == 0

            # simulate swiftclient tossing exceptions
            driver.Conn.mock_drop_connections(False)
