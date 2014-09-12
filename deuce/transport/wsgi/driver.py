from deuce import conf

from wsgiref import simple_server

import falcon

from deuce.transport.wsgi import v1_0
from deuce.transport.wsgi import hooks

from deuce import model


class Driver(object):

    def __init__(self):

        self.app = None
        self._init_routes()
        model.init_model()

    def before_hooks(self, req, resp, params):

        return [
            hooks.deucecontexthook(req, resp, params),
            hooks.transactionidhook(req, resp, params),
            hooks.projectidhook(req, resp, params),
            hooks.openstackhook(req, resp, params),
            hooks.openstackswifthook(req, resp, params)
        ]

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
        import deuce.util.log as logging
        msgtmpl = (u'Serving on host %(bind)s:%(port)s')
        logging.setup()
        logger = logging.getLogger(__name__)
        logger.info(msgtmpl,
                    {'bind': conf.server.host, 'port': int(conf.server.port)})

        httpd = simple_server.make_server(conf.server.host,
                                          int(conf.server.port),
                                          self.app)
        httpd.serve_forever()
