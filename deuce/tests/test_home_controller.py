from deuce.tests import ControllerTest


class TestHomeController(ControllerTest):

    def setUp(self):
        super(TestHomeController, self).setUp()
        self._hdrs = {"x-project-id": self.create_project_id()}

    def test_home_leaf(self):
        response = self.app.get('/v1.0/', headers=self._hdrs,
            expect_errors=True)
        self.assertEqual(response.status_int, 200)

        response = self.app.get('/v1.0/vaults', headers=self._hdrs,
            expect_errors=True)
        self.assertEqual(response.status_int, 302)

        response = self.app.get('/v1.0/notsupport', headers=self._hdrs,
            expect_errors=True)
        self.assertEqual(response.status_int, 404)
