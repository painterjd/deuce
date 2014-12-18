from cafe.engine.models import base

import json


class AuthToken(base.AutoMarshallingModel):

    def __init__(self, **kwargs):
        super(AuthToken, self).__init__()
        self.token = kwargs['access']['token']['id']
        self.tenantid = kwargs['access']['token']['tenant']['id']

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
