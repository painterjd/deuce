from tests.api import base
import ddt


class TestDiagnostics(base.TestBase):

    def setUp(self):
        super(TestDiagnostics, self).setUp()

    def test_ping(self):
        """Ping"""

        resp = self.client.ping()
        self.assert_204_response(resp)

    def test_health(self):
        """Health"""

        resp = self.client.health()
        self.assert_200_response(resp)

        # TODO: Add additional response.content validation
        resp_body = resp.json()
        self.assertTrue(resp_body[0].endswith('is active.'))

    def tearDown(self):
        super(TestDiagnostics, self).tearDown()
