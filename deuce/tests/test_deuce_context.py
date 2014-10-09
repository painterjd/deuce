import base64
import binascii
import falcon
import json
import mock
import re

import deuce
from deuce.transport.wsgi import hooks
from deuce.drivers import swift
from deuce.tests import HookTest


def before_hooks_swift(req, resp, params):
    return [
        hooks.DeuceContextHook(req, resp, params)
    ]


class DummyClassObject(object):
    pass


class TestDeuceContextHook(HookTest):

    def setUp(self):
        super(TestDeuceContextHook, self).setUp()

        deuce.context = None

    def tearDown(self):
        deuce.context = None

    def test_hook(self):
        self.assertIsNone(deuce.context)

        self.app_setup(before_hooks_swift)
        self.simulate_get('/v1.0')

        self.assertIsNotNone(deuce.context)
        self.assertIsNotNone(deuce.context.datacenter)
        self.assertIsInstance(deuce.context.datacenter, str)

        DATACENTER_REGEX = re.compile('^[a-z0-9_\-]+$')
        self.assertIsNotNone(
            DATACENTER_REGEX.match(deuce.context.datacenter))

    def test_hook_uppercase_dc(self):
        self.assertIsNone(deuce.context)

        deuce.conf.api_configuration.datacenter = \
            deuce.conf.api_configuration.datacenter.upper()

        self.app_setup(before_hooks_swift)
        self.simulate_get('/v1.0')

        self.assertIsNotNone(deuce.context)
        self.assertIsNotNone(deuce.context.datacenter)
        self.assertIsInstance(deuce.context.datacenter, str)

        DATACENTER_REGEX = re.compile('^[a-z0-9_\-]+$')
        self.assertIsNotNone(
            DATACENTER_REGEX.match(deuce.context.datacenter))
