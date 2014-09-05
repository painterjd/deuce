from tests.api import base
import ddt


class TestDiagnostics(base.TestBase):

    def setUp(self):
        super(TestDiagnostics, self).setUp()

    def test_ping(self):
        """Ping"""

        resp = self.client.ping()
        self.assertEqual(resp.status_code, 204,
                         'Status code returned for Ping: {0} . '
                         'Expected 204'.format(resp.status_code))
        self.assertHeaders(resp.headers)
        self.assertEqual(len(resp.content), 0,
                         'Response Content was not empty. Content: '
                         '{0}'.format(resp.content))

    def test_health(self):
        """Health"""

        resp = self.client.health()
        self.assertEqual(resp.status_code, 200,
                         'Status code returned for Health: {0} . '
                         'Expected 200'.format(resp.status_code))
        self.assertHeaders(resp.headers, json=True)

        # TODO: Add additional response.content validation
        resp_body = resp.json()
        self.assertTrue(resp_body[0].endswith('is active.'))

    def tearDown(self):
        super(TestDiagnostics, self).tearDown()
