import falcon


from deuce.transport.wsgi import v1_0
from deuce.transport.wsgi import hooks
from deuce.tests import V1Base


class TestHooks(V1Base):

    def app_setup(self, hooks):

        endpoints = [
            ('/v1.0', v1_0.public_endpoints()),

        ]
        self.app = falcon.API(before=hooks)

        for version_path, endpoints in endpoints:
            for route, resource in endpoints:
                self.app.add_route(version_path + route, resource)

    def test_openstack_hook(self):

        def before_hooks(req, resp, params):

            # Disk + Sqlite

            return [
                hooks.DeuceContextHook(req, resp, params),
                hooks.TransactionidHook(req, resp, params),
                hooks.OpenstackHook(req, resp, params)
            ]

        self.app_setup(before_hooks)

        response = self.simulate_get('/v1.0', headers={'X-Auth-Token': 'blah'})
        self.assertEqual(self.srmock.status, falcon.HTTP_200)

        response = self.simulate_get('/v1.0')
        self.assertEqual(self.srmock.status, falcon.HTTP_400)
