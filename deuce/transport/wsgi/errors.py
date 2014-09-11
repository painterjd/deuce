import falcon


class HTTPServiceUnavailable(falcon.HTTPServiceUnavailable):

    """Wraps falcon.HTTPServiceUnavailable"""

    TITLE = u'Service temporarily unavailable'

    def __init__(self, description):
        super(HTTPServiceUnavailable, self).__init__(
            self.TITLE)


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


class HTTPDocumentTypeNotSupported(HTTPBadRequestBody):

    """Wraps HTTPBadRequestBody with a standard description."""

    DESCRIPTION = u'Document type not supported.'

    def __init__(self):
        super(HTTPDocumentTypeNotSupported, self).__init__(self.DESCRIPTION)


class HTTPPreconditionFailed(falcon.HTTPPreconditionFailed):

    """Wraps HTTPPreconditionFailed with a contextual title."""

    TITLE = u'Precondition Failure'

    def __init__(self, description):
        super(HTTPPreconditionFailed, self).__init__(self.TITLE, description)


class HTTPNotFound(falcon.HTTPNotFound):

    """Wraps falcon.HTTPNotFound"""

    def __init__(self):
        super(HTTPNotFound, self).__init__()
