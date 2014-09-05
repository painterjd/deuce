import deuce

from deuce.hooks import HealthHook
from pecan.core import abort


class OpenStackHook(HealthHook):
    """Every request that hits Deuce must have a header specifying the
    auth_token for the user the request is for.

    If a request does not provide the header the request should fail
    with a 401"""

    def on_route(self, state):
        if super(OpenStackHook, self).health(state):
            return

        class OpenStackObject(object):
            pass

        deuce.context.openstack = OpenStackObject()

        # Enforce the existence of the x-auth-token header and assign
        # the value to the deuce context's open stack context
        try:

            # auth_token is assumed validated by an outside source (e.g Repose
            # or WSGI middleware
            deuce.context.openstack.auth_token = \
                state.request.headers['x-auth-token']
        except KeyError:
            # Invalid request
            abort(401, comment="Missing Headers : "
                               "X-Auth-Token",
                headers={
                    'Transaction-ID': deuce.context.transaction.request_id
                })
