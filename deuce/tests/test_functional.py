from deuce.tests import FunctionalTest


class TestRootController(FunctionalTest):

    def test_get(self):
        # Require project ID for root as well
        response = self.app.get('/', headers={},
            expect_errors=True)
        self.assertEqual(response.status_int, 400)

        response = self.app.get('/', headers={'x-project-id':
                                              self.create_project_id()},
            expect_errors=True)
        self.assertEqual(response.status_int, 404)

    def test_get_10(self):
        response = self.app.get('/v1.0', headers={'x-project-id':
                                                  self.create_project_id()})
        self.assertEqual(response.status_int, 302)

    def test_get_not_found(self):
        response = self.app.get('/a/bogus/url',
            headers={'x-project-id': self.create_project_id()},
            expect_errors=True)

        self.assertEqual(response.status_int, 404)
