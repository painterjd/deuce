import falcon

from deuce.tests import V1Base


class TestPing(V1Base):

    def test_ping(self):
        response = self.simulate_get('/v1.0/ping')
        self.assertEqual(self.srmock.status, falcon.HTTP_204)
        self.assertEqual(response, [])
