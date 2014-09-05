import uuid
from testfixtures import LogCapture

from deuce.tests import FunctionalTest
from deuce.util import log as logging


class TestLogging(FunctionalTest):

    def setUp(self):
        super(TestLogging, self).setUp()

        self._hdrs = {"X-Project-ID": self.create_project_id(),
                      "x-auth-token": self.create_auth_token()}

    def _testuuid(self, request_id):
        try:
            uuid.UUID(request_id.replace(request_id[:4], ''))
        except ValueError:
            return False
        else:
            return True

    def test_logging_handler(self):
        self.app.get('/v1.0/', headers=self._hdrs, expect_errors=True)

        # NOTE(TheSriram): Create a new LOG handler, and make sure the
        # the next time we try to create one, we receive the one created
        # earlier.

        LOG_new = logging.getLogger(__name__)
        LOG_exists = logging.getLogger(__name__)

        self.assertEqual(LOG_new, LOG_exists)

    def test_logging_requestid(self):
        self.app.get('/v1.0/', headers=self._hdrs, expect_errors=True)

        LOG = logging.getLogger(__name__)

        with LogCapture() as capture:
            LOG.info("Testing Request ID")

        self.assertTrue(self._testuuid(capture.records[0].request_id))
