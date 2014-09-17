import mock
import os
import sys

from pecan import conf
from swiftclient import client as Conn
from swiftclient.exceptions import ClientException

from deuce.drivers.swift import SwiftStorageDriver
from deuce.tests.test_disk_storage_driver import DiskStorageDriverTest


class SwiftStorageDriverTest(DiskStorageDriverTest):

    def create_driver(self):
        return SwiftStorageDriver()

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

    def test_network_drops(self):
        """
        This is only to exercise code that relies on network errors to occur
        """
        vault_id = 'notmatter'
        block_id = 'notmatter'

        driver = self.create_driver()

        # This should only run on SwiftStorageDriver and derivatives
        self.assertIsInstance(driver, SwiftStorageDriver)

        with mock.patch(
            'deuce.tests.db_mocking.swift_mocking.client.put_container'
        ) as put_container:
            put_container.side_effect = ClientException('mock')

            self.assertFalse(driver.create_vault(vault_id))

        with mock.patch(
            'deuce.tests.db_mocking.swift_mocking.client.head_container'
        ) as head_container:
            head_container.side_effect = ClientException('mock')

            self.assertFalse(driver.vault_exists(vault_id))

        with mock.patch(
            'deuce.tests.db_mocking.swift_mocking.client.delete_container'
        ) as delete_container:
            delete_container.side_effect = ClientException('mock')

            self.assertFalse(driver.delete_vault(vault_id))

        with mock.patch(
            'deuce.tests.db_mocking.swift_mocking.client.put_object'
        ) as put_object:
            put_object.side_effect = ClientException('mock')

            self.assertFalse(driver.store_block(vault_id, block_id,
                                                str('').encode('utf-8')))

        with mock.patch(
            'deuce.tests.db_mocking.swift_mocking.client.put_async_object'
        ) as put_async_object:
            put_async_object.side_effect = ClientException('mock')

            self.assertFalse(driver.store_async_block(
                             vault_id,
                             [block_id],
                             [str('').encode('utf-8')]))

        with mock.patch(
            'deuce.tests.db_mocking.swift_mocking.client.head_object'
        ) as head_object:
            head_object.side_effect = ClientException('mock')

            self.assertFalse(driver.block_exists(vault_id, block_id))

        with mock.patch(
            'deuce.tests.db_mocking.swift_mocking.client.delete_object'
        ) as delete_object:
            delete_object.side_effect = ClientException('mock')

            self.assertFalse(driver.delete_block(vault_id, block_id))

        with mock.patch(
            'deuce.tests.db_mocking.swift_mocking.client.get_object'
        ) as get_object:
            get_object.side_effect = ClientException('mock')

            self.assertIsNone(driver.get_block_obj(vault_id, block_id))

            self.assertEqual(driver.get_block_object_length(vault_id,
                                                            block_id),
                             0)

            get_object.side_effect = None
            get_object.return_value = ({'x-header': 'mock'}, False)

            self.assertIsNone(driver.get_block_obj(vault_id, block_id))

        # Stats should come back as zero even though the connection
        # "dropped"
        with mock.patch(
            'deuce.tests.db_mocking.swift_mocking.client.head_container'
        ) as head_container:
            head_container.side_effect = ClientException('mock')

            bad_vault_stats = driver.get_vault_statistics(vault_id)
            main_keys = ('total-size', 'block-count')
            for key in main_keys:
                assert key in bad_vault_stats.keys()
                assert bad_vault_stats[key] == 0
