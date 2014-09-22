import falcon
import mock
import binascii
import base64
import json
import deuce
from deuce.transport.wsgi import hooks
from deuce.drivers import swift
from deuce.tests import HookTest


def before_hooks_swift(req, resp, params):
    return [
        hooks.OpenstackSwiftHook(req, resp, params)
    ]


class DummyClassObject(object):
    pass


class TestOpenstackSwiftHook(HookTest):

    def setUp(self):
        super(TestOpenstackSwiftHook, self).setUp()
        self.datacenter = 'test'
        self.headers = {}

        deuce.context = DummyClassObject()
        deuce.context.datacenter = self.datacenter
        deuce.context.project_id = self.create_project_id()
        deuce.context.transaction = DummyClassObject()
        deuce.context.transaction.request_id = 'openstack-hook-test'
        deuce.context.openstack = DummyClassObject()

    def test_is_not_swift_driver(self):
        with mock.patch('deuce.storage_driver', object):
            self.app_setup(before_hooks_swift)
            self.simulate_get('/v1.0')

    def test_is_swift_driver(self):
        with mock.patch('deuce.storage_driver',
                        spec=swift.SwiftStorageDriver) as swift_driver:

            with mock.patch('deuce.transport.wsgi.hooks.'
                            'openstackswifthook.check_storage_url'
                            ) as hook_check_storage_url:
                self.app_setup(before_hooks_swift)
                hook_check_storage_url.return_value = True
                self.simulate_get('/v1.0')

    def test_missing_service_catalog(self):

        with mock.patch('deuce.storage_driver',
                        spec=swift.SwiftStorageDriver):
            self.app_setup(before_hooks_swift)
            response = self.simulate_get('/v1.0', headers=self.headers)
            self.assertEqual(self.srmock.status, falcon.HTTP_400)

    def test_has_service_catalog(self):
        with mock.patch('deuce.storage_driver',
                        spec=swift.SwiftStorageDriver) as swift_driver:
            with mock.patch('deuce.transport.wsgi.hooks.'
                            'openstackswifthook.decode_service_catalog'
                            ) as decode_catalog:
                decode_catalog.return_value = True

                with mock.patch('deuce.transport.wsgi.hooks.'
                                'openstackswifthook.find_storage_url'
                                ) as find_storage:
                    find_storage.return_value = 'test_url'

                    self.assertFalse(hasattr(deuce.context.openstack, 'swift'))
                    self.app_setup(before_hooks_swift)
                    self.simulate_get(
                        '/v1.0',
                        headers={
                            'x-service-catalog': 'mock'})

                    self.assertTrue(hasattr(deuce.context.openstack, 'swift'))
                    self.assertTrue(hasattr(deuce.context.openstack.swift,
                                            'storage_url'))
                    self.assertEqual(deuce.context.openstack.swift.storage_url,
                                     'test_url')

    def test_failed_base64_decode_service_catalog(self):
        with mock.patch('deuce.storage_driver',
                        spec=swift.SwiftStorageDriver) as swift_driver:
            with mock.patch('base64.b64decode') as b64_decoder:
                b64_decoder.side_effect = binascii.Error('mock')

                self.app_setup(before_hooks_swift)
                response = self.simulate_get(
                    '/v1.0',
                    headers={
                        'x-service-catalog': 'mock'})
                self.assertEqual(self.srmock.status, falcon.HTTP_412)

    def test_failed_json_decode_service_catalog(self):

        with mock.patch('deuce.storage_driver',
                        spec=swift.SwiftStorageDriver) as swift_driver:
            with mock.patch('base64.b64decode') as b64_decoder:
                b64_decoder.return_value = str('test-data').encode(
                    encoding='utf-8', errors='strict')
                self.app_setup(before_hooks_swift)
                response = self.simulate_get(
                    '/v1.0',
                    headers={
                        'x-service-catalog': 'mock'})
                self.assertEqual(self.srmock.status, falcon.HTTP_412)

    def test_json_decode_service_catalog(self):
        with mock.patch('deuce.storage_driver',
                        spec=swift.SwiftStorageDriver) as swift_driver:
            with mock.patch('base64.b64decode') as b64_decoder:
                b64_decoder.return_value = json.dumps(
                    {'hello': 'test'}).encode(encoding='utf-8',
                                              errors='strict')

                with mock.patch('deuce.transport.wsgi.hooks.'
                                'openstackswifthook.find_storage_url'
                                ) as find_storage:
                    find_storage.return_value = 'test_url'

                    self.assertFalse(hasattr(deuce.context.openstack, 'swift'))

                    self.app_setup(before_hooks_swift)
                    response = self.simulate_get(
                        '/v1.0',
                        headers={
                            'x-service-catalog': 'mock'})

                    self.assertTrue(hasattr(deuce.context.openstack, 'swift'))
                    self.assertTrue(hasattr(deuce.context.openstack.swift,
                                            'storage_url'))
                    self.assertEqual(deuce.context.openstack.swift.storage_url,
                                     'test_url')

    def test_find_storage_url_invalid_service_catalog(self):

        with mock.patch('deuce.storage_driver',
                        spec=swift.SwiftStorageDriver) as swift_driver:
            json_data = json.dumps({'hello': 'test'})
            byte_data = json_data.encode(encoding='utf-8', errors='strict')
            with mock.patch('base64.b64decode') as b64_decoder:
                b64_decoder.return_value = byte_data

                self.app_setup(before_hooks_swift)
                response = self.simulate_get(
                    '/v1.0',
                    headers={
                        'x-service-catalog': 'mock'})
                self.assertEqual(self.srmock.status, falcon.HTTP_412)

    def test_find_storage_url_invalid_service_catalog_with_access(self):
        with mock.patch('deuce.storage_driver',
                        spec=swift.SwiftStorageDriver) as swift_driver:
            with mock.patch('base64.b64decode') as b64_decoder:
                test_dict = {'access': {'hello': 'test'}}
                b64_decoder.return_value = json.dumps(test_dict).encode(
                    encoding='utf-8', errors='strict')
                self.app_setup(before_hooks_swift)
                response = self.simulate_get(
                    '/v1.0',
                    headers={
                        'x-service-catalog': 'mock'})
                self.assertEqual(self.srmock.status, falcon.HTTP_412)

    def test_find_storage_url_no_object_store(self):
        with mock.patch('deuce.storage_driver',
                        spec=swift.SwiftStorageDriver) as swift_driver:
            with mock.patch('base64.b64decode') as b64_decoder:
                b64_decoder.return_value = json.dumps(
                    self.create_service_catalog(
                        objectStoreType='mock')).encode(
                    encoding='utf-8', errors='strict')

                self.app_setup(before_hooks_swift)
                response = self.simulate_get(
                    '/v1.0',
                    headers={
                        'x-service-catalog': 'mock'})
                self.assertEqual(self.srmock.status, falcon.HTTP_412)

    def test_find_storage_url_no_endpoints(self):
        with mock.patch('deuce.storage_driver',
                        spec=swift.SwiftStorageDriver) as swift_driver:
            catalog = self.create_service_catalog(endpoints=False)
            json_data = json.dumps(catalog)
            byte_data = json_data.encode(encoding='utf-8', errors='strict')
            with mock.patch('base64.b64decode') as b64_decoder:
                b64_decoder.return_value = byte_data
                self.app_setup(before_hooks_swift)
                response = self.simulate_get(
                    '/v1.0',
                    headers={
                        'x-service-catalog': 'mock'})
                self.assertEqual(self.srmock.status, falcon.HTTP_412)

    def test_find_storage_url_no_region(self):
        with mock.patch('deuce.storage_driver',
                        spec=swift.SwiftStorageDriver) as swift_driver:
            with mock.patch('base64.b64decode') as b64_decoder:
                b64_decoder.return_value = json.dumps(
                    self.create_service_catalog(region='other')).encode(
                        encoding='utf-8', errors='strict')
                self.app_setup(before_hooks_swift)
                response = self.simulate_get(
                    '/v1.0',
                    headers={
                        'x-service-catalog': 'mock'})
                self.assertEqual(self.srmock.status, falcon.HTTP_412)

    def test_find_storage_url_final(self):
        catalog = self.create_service_catalog(region=self.datacenter,
                                              url='test_url')
        json_data = json.dumps(catalog)
        utf8_data = json_data.encode(encoding='utf-8', errors='strict')
        b64_data = base64.b64encode(utf8_data)

        with mock.patch('deuce.storage_driver',
                        spec=swift.SwiftStorageDriver) as swift_driver:

            self.assertFalse(hasattr(deuce.context.openstack, 'swift'))

            self.app_setup(before_hooks_swift)
            response = self.simulate_get(
                '/v1.0',
                headers={
                    'x-service-catalog': b64_data})

            self.assertTrue(hasattr(deuce.context.openstack, 'swift'))
            self.assertTrue(hasattr(deuce.context.openstack.swift,
                                    'storage_url'))
            self.assertEqual(deuce.context.openstack.swift.storage_url,
                             'test_url')
