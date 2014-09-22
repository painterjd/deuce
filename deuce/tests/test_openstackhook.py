import falcon


from deuce.transport.wsgi import hooks
from deuce.tests import HookTest


def before_hooks(req, resp, params):
    # Openstack Hook included
    return [
        hooks.DeuceContextHook(req, resp, params),
        hooks.TransactionidHook(req, resp, params),
        hooks.OpenstackHook(req, resp, params)
    ]


class TestOpenstackHook(HookTest):

    def test_openstack_hook(self):

        self.app_setup(before_hooks)

        response = self.simulate_get('/v1.0', headers={'X-Auth-Token': 'blah'})
        self.assertEqual(self.srmock.status, falcon.HTTP_200)

        response = self.simulate_get('/v1.0')
        self.assertEqual(self.srmock.status, falcon.HTTP_400)
