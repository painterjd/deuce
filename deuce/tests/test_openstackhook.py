from deuce.tests import HookTest

import deuce
from deuce.hooks import OpenStackHook

import webob.exc


class DummyClassObject(object):
    pass


class TestOpenStackHook(HookTest):

    def create_hook(self):
        return OpenStackHook()

    def setUp(self):
        super(TestOpenStackHook, self).setUp()
        self.state = DummyClassObject()
        self.state.request = DummyClassObject()
        self.state.request.headers = {}
        self.state.response = DummyClassObject()

        deuce.context = DummyClassObject()
        deuce.context.project_id = self.create_project_id()
        deuce.context.transaction = DummyClassObject()
        deuce.context.transaction.request_id = 'openstack-hook-test'

    def test_hook_health(self):
        hook = self.create_hook()
        self.state.request.path = '/v1.0/health'
        hook.on_route(self.state)

    def test_hook_ping(self):
        hook = self.create_hook()
        self.state.request.path = '/v1.0/ping'
        hook.on_route(self.state)

    def test_token_present(self):
        hook = self.create_hook()
        self.state.request.headers['x-auth-token'] = 'good'

        self.assertFalse(hasattr(deuce.context, 'openstack'))

        hook.on_route(self.state)

        self.assertTrue(hasattr(deuce.context, 'openstack'))

    def test_hook_not_present(self):
        hook = self.create_hook()
        with self.assertRaises(webob.exc.HTTPUnauthorized) \
                as expected_exception:
            hook.on_route(self.state)
