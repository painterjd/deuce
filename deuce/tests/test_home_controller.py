from unittest import TestCase
from webtest import TestApp
from deuce.tests import FunctionalTest
import os
import hashlib
from random import randrange

import json


class TestHomeController(FunctionalTest):

    def setUp(self):
        super(TestHomeController, self).setUp()
        self._hdrs = {"x-project-id": self.create_project_id(),
                      "x-auth-token": self.create_auth_token()}

    def test_home_leaf(self):
        response = self.app.get('/v1.0/', headers=self._hdrs,
            expect_errors=True)
        assert response.status_int == 200

        response = self.app.get('/v1.0/vaults', headers=self._hdrs,
            expect_errors=True)
        assert response.status_int == 302

        response = self.app.get('/v1.0/notsupport', headers=self._hdrs,
            expect_errors=True)
        assert response.status_int == 404
