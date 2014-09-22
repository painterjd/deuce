import falcon

from deuce.tests import V1Base


class TestHealth(V1Base):

    def test_health(self):
        response = self.simulate_get('/v1.0/health')
        self.assertEqual(self.srmock.status, falcon.HTTP_200)
