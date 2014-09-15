import deuce
from deuce.transport.wsgi.hooks import healthpingcheck
from deuce.transport.wsgi.hooks import OpenStackObject


@healthpingcheck
def OpenstackHook(req, resp, params):
    deuce.context.openstack = OpenStackObject()
    deuce.context.openstack.auth_token = req.get_header('x-auth-token',
                                                        required=True)