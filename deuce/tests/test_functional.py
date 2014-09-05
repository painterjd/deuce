from unittest import TestCase
from webtest import TestApp
from deuce.tests import FunctionalTest


class TestRootController(FunctionalTest):

    def test_get(self):
        # Require project ID for root as well
        response = self.app.get('/', headers={'x-auth-token':
                                              self.create_auth_token()},
            expect_errors=True)
        assert response.status_int == 400

        # Require auth token for root as well
        response = self.app.get('/', headers={'x-project-id':
                                              self.create_project_id()},
            expect_errors=True)
        assert response.status_int == 401

        response = self.app.get('/', headers={'x-project-id':
                                              self.create_project_id(),
            'x-auth-token': self.create_auth_token()},
            expect_errors=True)
        assert response.status_int == 404

    def test_get_10(self):
        response = self.app.get('/v1.0', headers={'x-project-id':
                                                  self.create_project_id(),
            'x-auth-token': self.create_auth_token()})
        assert response.status_int == 302

    def test_get_not_found(self):
        response = self.app.get('/a/bogus/url',
            headers={'x-project-id': self.create_project_id(),
                     'x-auth-token': self.create_auth_token()},
            expect_errors=True)

        assert response.status_int == 404
