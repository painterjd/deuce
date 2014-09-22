'''

This file contains exceptions that can be thrown by the model, which
will in turn be caught by the endpoint resource and converted into an
appropriate status code

'''


class ConsistencyError(Exception):
    pass
