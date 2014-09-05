from unittest import TestCase
from webtest import TestApp
from deuce.tests import FunctionalTest

import json


class TestHealthController(FunctionalTest):

    def setUp(self):
        super(TestHealthController, self).setUp()
        # health end point does not require token or projectid headers!
        self._hdrs = {}

    def test_health(self):
        path = '/v1.0/health'
        response = self.app.get(path, headers=self._hdrs)
        assert response.status_int == 200

        response = self.app.put(path, headers=self._hdrs,
                expect_errors=True)
        self.assertEqual(response.status_int, 404)

    def test_ping(self):
        response = self.app.get('/v1.0/ping', headers=self._hdrs)
        assert response.status_int == 204
