import six
import base64
import binascii
import json
from pecan.hooks import PecanHook
from deuce.hooks import HealthHook
from deuce.hooks.openstackhook import OpenStackObject
from pecan.core import abort
from deuce.drivers import swift
import deuce


class OpenStackSwiftHook(HealthHook):
    """Every request that hits Deuce must have a header specifying the
    x-storage-url if running the swift storage driver

    If a request does not provide the header the request should fail
    with a 401"""

    def decode_service_catalog(self, catalog):
        """Decode a JSON-based Base64 encoded Service Catalog
        """
        try:
            data = base64.b64decode(catalog)
            utf8_data = data.decode(encoding='utf-8', errors='strict')

        except binascii.Error:
            abort(412, comment="X-Service-Catalog invalid encoding",
                  headers={
                      'Transaction-ID': deuce.context.transaction.request_id
                  })

        try:
            json_data = json.loads(utf8_data)
        except ValueError:
            abort(412, comment="X-Service-Catalog: invalid format",
                  headers={
                      'Transaction-ID': deuce.context.transaction.request_id
                  })

        return json_data

    def find_storage_url(self, catalog):
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

            abort(412, comment="X-Service-Catalog: missing object-store",
                  headers={
                      'Transaction-ID': deuce.context.transaction.request_id
                  })

        except (KeyError, LookupError):
            abort(412, comment="X-Service-Catalog: invalid service catalog",
                  headers={
                      'Transaction-ID': deuce.context.transaction.request_id
                  })

    def check_storage_url(self, state):

        deuce.context.openstack.swift = OpenStackObject()

        # X-Service-Catalog if present assumed validated by outside source
        if 'x-service-catalog' in state.request.headers:
            catalog_data = self.decode_service_catalog(
                state.request.headers['x-service-catalog'])

            private_storage = self.find_storage_url(catalog_data)

            # Default Storage URL is the internal network
            deuce.context.openstack.swift.storage_url = private_storage

        else:
            # Invalid request
            abort(412, comment="Missing Headers : "
                               "X-Service-Catalog",
                headers={
                    'Transaction-ID': deuce.context.transaction.request_id
                })

    def on_route(self, state):
        if super(OpenStackSwiftHook, self).health(state):
            return

        # Enforce the existence of the x-storage-url header and assign
        # the value to the deuce context's open stack context, if the
        # current storage driver is swift

        if isinstance(deuce.storage_driver,
                      swift.SwiftStorageDriver):  # pragma: no cover
            self.check_storage_url(state)
        else:
            pass
