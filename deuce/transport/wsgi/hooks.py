import deuce.util.log as logging
logger = logging.getLogger(__name__)
from functools import wraps
import falcon


import base64
import binascii
import json
from deuce.common import local
from deuce.common import context
from deuce.drivers import swift
from deuce.transport.wsgi import errors
import deuce


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


def deucecontexthook(req, resp, params):
    """
    Deuce Context Hook
    """
    from threading import local as local_factory
    deuce.context = local_factory()


@healthpingcheck
def projectidhook(req, resp, params):
    deuce.context.project_id = req.get_header('x-project-id', required=True)


def transactionidhook(req, resp, params):
    transaction = context.RequestContext()
    setattr(local.store, 'context', transaction)
    deuce.context.transaction = transaction
    resp.set_header('Transaction-ID', deuce.context.transaction.request_id)


@healthpingcheck
def openstackhook(req, resp, params):
    deuce.context.openstack = OpenStackObject()
    deuce.context.openstack.auth_token = req.get_header('x-auth-token',
                                                        required=True)


@healthpingcheck
def openstackswifthook(req, resp, params):
    def decode_service_catalog(catalog):
        """Decode a JSON-based Base64 encoded Service Catalog
        """
        try:
            data = base64.b64decode(catalog)
            utf8_data = data.decode(encoding='utf-8', errors='strict')

        except binascii.Error:
            raise errors.HTTPPreconditionFailed(
                "X-Service-Catalog invalid encoding")

        try:
            json_data = json.loads(utf8_data)
        except ValueError:
            raise errors.HTTPPreconditionFailed(
                "X-Service-Catalog invalid format")

        return json_data

    def find_storage_url(catalog):
        try:
            main_catalog = catalog
            if 'access' in catalog:
                main_catalog = catalog['access']

            service_catalog = main_catalog['serviceCatalog']
            for service in service_catalog:
                if service['type'] == 'object-store':
                    for endpoint in service['endpoints']:
                        if endpoint['region'].lower() == \
                                deuce.context.datacenter:
                            return endpoint['internalURL']

            raise errors.HTTPPreconditionFailed(
                "X-Service-Catalog missing object store")

        except (KeyError, LookupError):
            raise errors.HTTPPreconditionFailed(
                "X-Service-Catalog invalid service catalog")

    def check_storage_url(req):

        deuce.context.openstack.swift = OpenStackObject()

        # X-Service-Catalog if present assumed validated by outside source

        catalog_data = decode_service_catalog(
            req.get_header(
                'x-service-catalog',
                required=True))

        private_storage = find_storage_url(catalog_data)

        # Default Storage URL is the internal network
        deuce.context.openstack.swift.storage_url = private_storage

    if isinstance(deuce.storage_driver,
                  swift.SwiftStorageDriver):  # pragma: no cover
        check_storage_url(req)
    else:
        pass
