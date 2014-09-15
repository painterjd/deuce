import base64
import binascii
import json

import deuce
from deuce.drivers import swift
from deuce.transport.wsgi.hooks import healthpingcheck
from deuce.transport.wsgi.hooks import OpenStackObject
from deuce.transport.wsgi import errors


@healthpingcheck
def OpenstackSwiftHook(req, resp, params):
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
