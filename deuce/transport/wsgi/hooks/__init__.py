from functools import wraps



class OpenStackObject(object):
    """
    Dummy object for the Deuce Context structure
    """
    pass


def healthpingcheck(func):
    @wraps(func)
    def wrap(*args, **kwargs):
        if args[0].relative_uri == '/v1.0/health' \
                or args[0].relative_uri == '/v1.0/ping':
            return
        else:
            func(*args, **kwargs)
    return wrap

from deuce.transport.wsgi.hooks.deucecontexthook import DeuceContextHook
from deuce.transport.wsgi.hooks.openstackhook import OpenstackHook
from deuce.transport.wsgi.hooks.openstackswifthook import OpenstackSwiftHook
from deuce.transport.wsgi.hooks.projectidhook import ProjectidHook
from deuce.transport.wsgi.hooks.transactionidhook import TransactionidHook