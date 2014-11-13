import json

import falcon

import deuce.util.log as logging
from deuce.model import Health


logger = logging.getLogger(__name__)


class CollectionResource(object):

    def on_get(self, req, resp):
        resp.body = json.dumps(Health.health())
