from cafe.engine.models import base

import json


class AuthToken(base.AutoMarshallingModel):

    def __init__(self, **kwargs):
        super(AuthToken, self).__init__()
        self.token = kwargs['access']['token']['id']
        self.regions = dict()

        # retrieve cloud files information - tenantid, storage urls
        for entry in kwargs['access']['serviceCatalog']:
            if entry['type'] == 'object-store':
                endpoints = entry['endpoints']
                for endpoint in endpoints:
                    region_data = dict()
                    region_data['internalURL'] = endpoint['internalURL']
                    region_data['publicURL'] = endpoint['publicURL']
                    region_data['tenantId'] = endpoint['tenantId']
                    self.regions[endpoint['region']] = region_data
                break

    @classmethod
    def _json_to_obj(cls, serialized_str):
        ret = None
        json_dict = json.loads(serialized_str)
        ret = cls._dict_to_obj(json_dict)
        return ret

    @classmethod
    def _dict_to_obj(cls, auth_dict):
        auth_resp = AuthToken(**auth_dict)
        return auth_resp
