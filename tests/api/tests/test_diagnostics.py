from tests.api import base
import ddt


class TestDiagnostics(base.TestBase):
    _multiprocess_can_split_ = True

    def setUp(self):
        super(TestDiagnostics, self).setUp()

    def test_ping(self):
        """Ping"""

        if not self.skip_diagnostics:
            resp = self.client.ping()
            self.assert_204_response(resp)
        else:
            self.skipTest('Configuration value skip_diagnostics = True')

    def test_health(self):
        """Health"""

        if not self.skip_diagnostics:
            resp = self.client.health()
            self.assert_200_response(resp)

            resp_body = resp.json()
            self.assertTrue(resp_body[0].find('is active') > -1)
        else:
            self.skipTest('Configuration value skip_diagnostics = True')

    def tearDown(self):
        super(TestDiagnostics, self).tearDown()
