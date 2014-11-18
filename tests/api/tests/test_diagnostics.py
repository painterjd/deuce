from tests.api import base
import ddt


class TestDiagnostics(base.TestBase):
    _multiprocess_can_split_ = True

    def setUp(self):
        super(TestDiagnostics, self).setUp()

    def test_ping(self):
        """Ping"""

        if self.skip_diagnostics:
            self.skipTest('Configuration value skip_diagnostics = True')
        resp = self.client.ping()
        self.assert_204_response(resp)

    def test_health(self):
        """Health"""

        if self.skip_diagnostics:
            self.skipTest('Configuration value skip_diagnostics = True')
        resp = self.client.health()
        self.assert_200_response(resp)

        resp_body = resp.json()
        self.assertTrue(resp_body[0].find('is active') > -1)

    def tearDown(self):
        super(TestDiagnostics, self).tearDown()
