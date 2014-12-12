import falcon
from falcon import http_error
import falcon.status_codes as status


class HTTPInternalServerError(falcon.HTTPInternalServerError):

    """Wraps falcon.HTTPServiceUnavailable"""

    TITLE = u'Service temporarily unavailable'

    def __init__(self, description, **kwargs):
        super(HTTPInternalServerError, self).__init__(
            self.TITLE, description=description, **kwargs)


class HTTPBadRequestAPI(falcon.HTTPBadRequest):

    """Wraps falcon.HTTPBadRequest with a contextual title."""

    TITLE = u'Invalid API request'

    def __init__(self, description):
        super(HTTPBadRequestAPI, self).__init__(self.TITLE, description)


class HTTPBadRequestBody(falcon.HTTPBadRequest):

    """Wraps falcon.HTTPBadRequest with a contextual title."""

    TITLE = u'Invalid request body'

    def __init__(self, description):
        super(HTTPBadRequestBody, self).__init__(self.TITLE, description)


class HTTPConflict(falcon.HTTPConflict):

    """Wraps falcon.HTTPConflict with a contextual title."""

    TITLE = u'Conflict'

    def __init__(self, description):
        super(HTTPConflict, self).__init__(self.TITLE, description)


class HTTPGone(http_error.HTTPError):

    """ Resource gone  """

    TITLE = u'Gone'

    def __init__(self, description, **kwargs):
        # (BenjamenMeyer) For some reason we cannot use super() here.
        # If we do, then it complains. May be something can be fixed
        # in Falcon to change it so we can.
        http_error.HTTPError.__init__(self,
                                      status.HTTP_410,
                                      self.TITLE,
                                      description=description,
                                      **kwargs)


class HTTPPreconditionFailed(falcon.HTTPPreconditionFailed):

    """Wraps HTTPPreconditionFailed with a contextual title."""

    TITLE = u'Precondition Failure'

    def __init__(self, description):
        super(HTTPPreconditionFailed, self).__init__(self.TITLE, description)


class HTTPNotFound(falcon.HTTPNotFound):

    """Wraps falcon.HTTPNotFound"""

    def __init__(self):
        super(HTTPNotFound, self).__init__()


class HTTPMethodNotAllowed(falcon.HTTPMethodNotAllowed):

    """Wraps falcon.HTTPMethodNotAllowed"""

    TITLE = u'Method Not Allowed'

    def __init__(self, allowed_method, description):
        super(HTTPMethodNotAllowed, self).__init__(allowed_method,
                                                   description=description)
