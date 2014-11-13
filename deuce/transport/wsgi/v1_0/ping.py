import falcon

import deuce.util.log as logging


logger = logging.getLogger(__name__)


class CollectionResource(object):

    def on_get(self, req, resp):
        resp.status = falcon.HTTP_204
