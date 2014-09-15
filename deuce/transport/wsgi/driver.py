from deuce import conf

from wsgiref import simple_server

import falcon

from deuce.transport.wsgi import v1_0
from deuce.transport.wsgi import hooks

from deuce import model
import deuce.util.log as logging


class Driver(object):

    def __init__(self):

        self.app = None
        self._init_routes()
        model.init_model()

    def before_hooks(self, req, resp, params):

        # Disk + Sqlite

        return [
            hooks.DeuceContextHook(req, resp, params),
            hooks.TransactionidHook(req, resp, params),
            hooks.ProjectidHook(req, resp, params)
        ]

        # Swift

        # return [
        #     hooks.DeuceContextHook(req, resp, params),
        #     hooks.TransactionidHook(req, resp, params),
        #     hooks.ProjectidHook(req, resp, params),
        #     hooks.OpenstackHook(req, resp, params),
        #     hooks.OpenstackSwiftHook(req, resp, params)
        # ]

    def _init_routes(self):
        """Initialize hooks and URI routes to resources."""

        endpoints = [
            ('/v1.0', v1_0.public_endpoints()),

        ]

        self.app = falcon.API(before=self.before_hooks)

        for version_path, endpoints in endpoints:
            for route, resource in endpoints:
                self.app.add_route(version_path + route, resource)

    def listen(self):
        """Self-host using 'bind' and 'port' from deuce conf"""
        msgtmpl = (u'Serving on host %(bind)s:%(port)s')
        logger = logging.getLogger(__name__)
        logger.info(msgtmpl,
                    {'bind': conf.server.host, 'port': conf.server.port})

        httpd = simple_server.make_server(conf.server.host,
                                          conf.server.port,
                                          self.app)
        httpd.serve_forever()
