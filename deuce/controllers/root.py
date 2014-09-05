from pecan import expose, redirect, response
from webob.exc import status_map

from deuce.controllers.home import HomeController


class RootController(object):

    def __init__(self):
        # Support multiple API versions from the beginning. This
        # should most likely get moved into config
        self.versions = {"v1.0": HomeController()}

    # DISABLE INDEX PAGE!
    # def index(self):

    @expose()
    def _lookup(self, primary_key, *remainder):
        try:
            return self.versions[primary_key], remainder
        except KeyError:
            response.status_code = 404

    @expose('json')
    def error(self, status):
        try:
            status = int(status)
        except ValueError:  # pragma: no cover
            status = 500

        message = getattr(status_map.get(status), 'explanation', '')
        return dict(status=status, message=message)
