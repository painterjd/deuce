import falcon
from falcon import api_helpers
import msgpack
import six
import json
from six.moves.urllib.parse import urlparse, parse_qs
from deuce.util import set_qs
from deuce.model import Vault
# import deuce.transport.wsgi.errors as wsgi_errors
import deuce.util.log as logging
from deuce.transport.validation import *
from deuce.util.filecat import FileCat
import deuce
logger = logging.getLogger(__name__)


class CollectionResource(object):

    def on_get(self, req, resp):
        resp.status = falcon.HTTP_204
