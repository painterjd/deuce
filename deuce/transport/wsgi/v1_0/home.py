import json

import falcon

import deuce.util.log as logging


logger = logging.getLogger(__name__)


class Resource(object):

    def on_get(self, req, resp):
        # TODO(TheSriram): Must return a home document
        resp.status = falcon.HTTP_200
        resp.body = json.dumps({})
