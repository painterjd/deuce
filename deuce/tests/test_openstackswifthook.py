from deuce.tests import HookTest

import deuce
from deuce.hooks import OpenStackSwiftHook
from deuce.drivers import swift

import webob.exc
import mock
import json
import base64
import binascii


class DummyClassObject(object):
    pass


class TestOpenStackSwiftHook(HookTest):

    def create_hook(self):
        return OpenStackSwiftHook()

    def setUp(self):
        super(TestOpenStackSwiftHook, self).setUp()
        self.state = DummyClassObject()
        self.state.request = DummyClassObject()
        self.state.request.headers = {}
        self.state.response = DummyClassObject()

        self.datacenter = 'test'

        import deuce
        deuce.context = DummyClassObject()
        deuce.context.datacenter = self.datacenter
        deuce.context.project_id = self.create_project_id()
        deuce.context.transaction = DummyClassObject()
        deuce.context.transaction.request_id = 'openstack-hook-test'
        deuce.context.openstack = DummyClassObject()

    def tearDown(self):
        super(TestOpenStackSwiftHook, self).tearDown()

    def test_hook_health(self):
        hook = self.create_hook()
        self.state.request.path = '/v1.0/health'
        hook.on_route(self.state)

    def test_hook_ping(self):
        hook = self.create_hook()
        self.state.request.path = '/v1.0/ping'
        hook.on_route(self.state)

    def test_is_not_swift_driver(self):
        hook = self.create_hook()
        with mock.patch('deuce.storage_driver', object) as swift_driver:
            hook.on_route(self.state)

    def test_is_swift_driver(self):
        hook = self.create_hook()
        with mock.patch('deuce.storage_driver',
                        spec=swift.SwiftStorageDriver) as swift_driver:

            with mock.patch('deuce.hooks.openstackswifthook.'
                            'OpenStackSwiftHook.check_storage_url'
                            ) as hook_check_storage_url:
                hook_check_storage_url.return_value = True

                hook.on_route(self.state)

    def test_missing_service_catalog(self):
        hook = self.create_hook()
        with mock.patch('deuce.storage_driver',
                        spec=swift.SwiftStorageDriver) as swift_driver:
            with self.assertRaises(webob.exc.HTTPPreconditionFailed) \
                    as expected_exception:
                hook.on_route(self.state)

    def test_has_service_catalog(self):
        hook = self.create_hook()
        self.state.request.headers['x-service-catalog'] = True
        with mock.patch('deuce.storage_driver',
                        spec=swift.SwiftStorageDriver) as swift_driver:
            with mock.patch('deuce.hooks.openstackswifthook.'
                            'OpenStackSwiftHook.decode_service_catalog'
                            ) as decode_catalog:
                decode_catalog.return_value = True

                with mock.patch('deuce.hooks.openstackswifthook.'
                                'OpenStackSwiftHook.find_storage_url'
                                ) as find_storage:
                    find_storage.return_value = 'test_url'

                    self.assertFalse(hasattr(deuce.context.openstack, 'swift'))

                    hook.on_route(self.state)

                    self.assertTrue(hasattr(deuce.context.openstack, 'swift'))
                    self.assertTrue(hasattr(deuce.context.openstack.swift,
                                            'storage_url'))
                    self.assertEqual(deuce.context.openstack.swift.storage_url,
                                    'test_url')

    def test_failed_base64_decode_service_catalog(self):
        hook = self.create_hook()
        self.state.request.headers['x-service-catalog'] = True
        with mock.patch('deuce.storage_driver',
                        spec=swift.SwiftStorageDriver) as swift_driver:
            with mock.patch('base64.b64decode') as b64_decoder:
                b64_decoder.side_effect = binascii.Error('mock')

                with self.assertRaises(webob.exc.HTTPPreconditionFailed) \
                        as expected_exception:

                    hook.on_route(self.state)

    def test_failed_json_decode_service_catalog(self):
        hook = self.create_hook()
        self.state.request.headers['x-service-catalog'] = True
        with mock.patch('deuce.storage_driver',
                        spec=swift.SwiftStorageDriver) as swift_driver:
            with mock.patch('base64.b64decode') as b64_decoder:
                b64_decoder.return_value = str('test-data').encode(
                    encoding='utf-8', errors='strict')

                with self.assertRaises(webob.exc.HTTPPreconditionFailed) \
                        as expected_exception:

                    hook.on_route(self.state)

    def test_json_decode_service_catalog(self):
        hook = self.create_hook()
        self.state.request.headers['x-service-catalog'] = True
        with mock.patch('deuce.storage_driver',
                        spec=swift.SwiftStorageDriver) as swift_driver:
            with mock.patch('base64.b64decode') as b64_decoder:
                b64_decoder.return_value = json.dumps(
                    {'hello': 'test'}).encode(encoding='utf-8',
                                              errors='strict')

                with mock.patch('deuce.hooks.openstackswifthook.'
                                'OpenStackSwiftHook.find_storage_url'
                                ) as find_storage:
                    find_storage.return_value = 'test_url'

                    self.assertFalse(hasattr(deuce.context.openstack, 'swift'))

                    hook.on_route(self.state)

                    self.assertTrue(hasattr(deuce.context.openstack, 'swift'))
                    self.assertTrue(hasattr(deuce.context.openstack.swift,
                                            'storage_url'))
                    self.assertEqual(deuce.context.openstack.swift.storage_url,
                                    'test_url')

    def test_find_storage_url_invalid_service_catalog(self):
        hook = self.create_hook()
        self.state.request.headers['x-service-catalog'] = True
        with mock.patch('deuce.storage_driver',
                        spec=swift.SwiftStorageDriver) as swift_driver:
            json_data = json.dumps({'hello': 'test'})
            byte_data = json_data.encode(encoding='utf-8', errors='strict')
            with mock.patch('base64.b64decode') as b64_decoder:
                b64_decoder.return_value = byte_data
                with self.assertRaises(webob.exc.HTTPPreconditionFailed) \
                        as expected_exception:

                    hook.on_route(self.state)

    def test_find_storage_url_invalid_service_catalog_with_access(self):
        hook = self.create_hook()
        self.state.request.headers['x-service-catalog'] = True
        with mock.patch('deuce.storage_driver',
                        spec=swift.SwiftStorageDriver) as swift_driver:
            with mock.patch('base64.b64decode') as b64_decoder:
                test_dict = {'access': {'hello': 'test'}}
                b64_decoder.return_value = json.dumps(test_dict).encode(
                    encoding='utf-8', errors='strict')

                with self.assertRaises(webob.exc.HTTPPreconditionFailed) \
                        as expected_exception:

                    hook.on_route(self.state)

    def test_find_storage_url_no_object_store(self):
        hook = self.create_hook()
        self.state.request.headers['x-service-catalog'] = True
        with mock.patch('deuce.storage_driver',
                        spec=swift.SwiftStorageDriver) as swift_driver:
            with mock.patch('base64.b64decode') as b64_decoder:
                b64_decoder.return_value = json.dumps(
                    self.create_service_catalog(objectStoreType=''))

                with self.assertRaises(webob.exc.HTTPPreconditionFailed) \
                        as expected_exception:

                    hook.on_route(self.state)

    def test_find_storage_url_no_object_store(self):
        hook = self.create_hook()
        self.state.request.headers['x-service-catalog'] = True
        with mock.patch('deuce.storage_driver',
                        spec=swift.SwiftStorageDriver) as swift_driver:
            catalog = self.create_service_catalog(objectStoreType='other')
            json_data = json.dumps(catalog)
            byte_data = json_data.encode(encoding='utf-8', errors='strict')
            with mock.patch('base64.b64decode') as b64_decoder:
                b64_decoder.return_value = byte_data

                with self.assertRaises(webob.exc.HTTPPreconditionFailed) \
                        as expected_exception:

                    hook.on_route(self.state)

    def test_find_storage_url_no_endpoints(self):
        hook = self.create_hook()
        self.state.request.headers['x-service-catalog'] = True
        with mock.patch('deuce.storage_driver',
                        spec=swift.SwiftStorageDriver) as swift_driver:
            catalog = self.create_service_catalog(endpoints=False)
            json_data = json.dumps(catalog)
            byte_data = json_data.encode(encoding='utf-8', errors='strict')
            with mock.patch('base64.b64decode') as b64_decoder:
                b64_decoder.return_value = byte_data
                with self.assertRaises(webob.exc.HTTPPreconditionFailed) \
                        as expected_exception:

                    hook.on_route(self.state)

    def test_find_storage_url_no_region(self):
        hook = self.create_hook()
        self.state.request.headers['x-service-catalog'] = True
        with mock.patch('deuce.storage_driver',
                        spec=swift.SwiftStorageDriver) as swift_driver:
            with mock.patch('base64.b64decode') as b64_decoder:
                b64_decoder.return_value = json.dumps(
                    self.create_service_catalog(region='other')).encode(
                        encoding='utf-8', errors='strict')

                with self.assertRaises(webob.exc.HTTPPreconditionFailed) \
                        as expected_exception:

                    hook.on_route(self.state)

    def test_find_storage_url_final(self):
        hook = self.create_hook()

        catalog = self.create_service_catalog(region=self.datacenter,
                                              url='test_url')
        json_data = json.dumps(catalog)
        utf8_data = json_data.encode(encoding='utf-8', errors='strict')
        b64_data = base64.b64encode(utf8_data)
        self.state.request.headers['x-service-catalog'] = b64_data

        with mock.patch('deuce.storage_driver',
                        spec=swift.SwiftStorageDriver) as swift_driver:

            self.assertFalse(hasattr(deuce.context.openstack, 'swift'))

            hook.on_route(self.state)

            self.assertTrue(hasattr(deuce.context.openstack, 'swift'))
            self.assertTrue(hasattr(deuce.context.openstack.swift,
                                    'storage_url'))
            self.assertEqual(deuce.context.openstack.swift.storage_url,
                            'test_url')
