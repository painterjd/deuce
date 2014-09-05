import uuid

from cafe.engine.http import client
from tests.api.utils.models import auth_requests, auth_response


class AuthClient(client.AutoMarshallingHTTPClient):
    """
    Client Objects for Authentication
    """

    def __init__(self, url, serialize_format='json',
                 deserialize_format='json'):
        super(AuthClient, self).__init__(serialize_format, deserialize_format)
        self.url = url
        self.serialize_format = serialize_format
        self.deserialize_format = deserialize_format

        self.default_headers['Accept'] = 'application/{0}' \
                                         ''.format(deserialize_format)

    def get_auth_token(self, username, api_key):
        """
        Get Authentication Token using Username + Api Key
        """
        request_obj = auth_requests.AuthUsernameApiKey(username, api_key)
        resp = self.request('POST', self.url + '/tokens',
                            request_entity=request_obj,
                            response_entity_type=auth_response.AuthToken)
        return resp


class BaseDeuceClient(client.AutoMarshallingHTTPClient):
    """
    Client Objects for Deuce
    """

    def __init__(self, url, version, auth_token=None, storage_url=None,
                 tenantid=None, serialize_format='json',
                 deserialize_format='json'):
        super(BaseDeuceClient, self).__init__(serialize_format,
                                              deserialize_format)
        self.url = url
        self.version = version
        self.auth_token = ''
        if auth_token:
            self.auth_token = auth_token
        self.default_headers['X-Storage-Url'] = storage_url
        self.default_headers['X-Auth-Token'] = self.auth_token
        if tenantid:
            self.default_headers['X-Project-Id'] = tenantid
        else:
            self.default_headers['X-Project-Id'] = str(uuid.uuid4())

    def create_vault(self, vaultname):
        """
        Create a Vault
        """
        resp = self.request('PUT', '{0}/{1}/vaults/{2}'.format(self.url,
                                                               self.version,
                                                               vaultname))
        return resp

    def delete_vault(self, vaultname):
        """
        Delete a Vault
        """

        resp = self.request('DELETE', '{0}/{1}/vaults/{2}'.format(self.url,
                                                                  self.version,
                                                                  vaultname))
        return resp

    def get_vault(self, vaultname):
        """
        Get a vault
        """

        resp = self.request('GET', '{0}/{1}/vaults/{2}'.format(self.url,
                                                               self.version,
                                                               vaultname))
        return resp

    def vault_head(self, vaultname):
        """
        Get vault statistics via HEAD
        """

        resp = self.request('HEAD', '{0}/{1}/vaults/{2}'.format(self.url,
                                                                self.version,
                                                                vaultname))
        return resp

    def list_of_vaults(self, marker=None, limit=None, alternate_url=None):
        """
        Get a list of all vaults
        """

        parameters = {}
        if marker is not None:
            parameters['marker'] = marker
        if limit is not None:
            parameters['limit'] = limit
        if alternate_url:
            url = alternate_url
        else:
            url = '{0}/{1}/vaults'.format(self.url, self.version)
        resp = self.request('GET', url, params=parameters)
        return resp

    def list_of_blocks(self, vaultname=None, marker=None, limit=None,
                       alternate_url=None):
        """
        Get a list of all blocks
        """

        parameters = {}
        if marker is not None:
            parameters['marker'] = marker
        if limit is not None:
            parameters['limit'] = limit
        if alternate_url:
            url = alternate_url
        else:
            url = '{0}/{1}/vaults/{2}/blocks'.format(self.url, self.version,
                                                     vaultname)
        resp = self.request('GET', url, params=parameters)
        return resp

    def upload_block(self, vaultname, blockid, block_data):
        """
        Upload a block
        """

        new_header = {'Content-Type': 'application/octet-stream',
                      'content-length': len(block_data)}
        resp = self.request('PUT', '{0}/{1}/vaults/{2}/blocks/{3}'.format(
            self.url, self.version, vaultname, blockid),
            headers=new_header, data=block_data)
        return resp

    def upload_multiple_blocks(self, vaultname, content):
        """
        Upload multiple blocks using msgpacked content
        """

        new_header = {'Content-Type': 'application/msgpack',
                      'content-length': len(content)}
        resp = self.request('POST', '{0}/{1}/vaults/{2}/blocks'.format(
            self.url, self.version, vaultname),
            headers=new_header, data=content)
        return resp

    def delete_block(self, vaultname, blockid):
        """
        Delete a block
        """

        resp = self.request('DELETE', '{0}/{1}/vaults/{2}/blocks/{3}'.format(
            self.url, self.version, vaultname, blockid))
        return resp

    def get_block(self, vaultname, blockid):
        """
        Get data of a block
        """

        resp = self.request('GET', '{0}/{1}/vaults/{2}/blocks/{3}'.format(
            self.url, self.version, vaultname, blockid))
        return resp

    def create_file(self, vaultname):
        """
        Create a file
        """

        resp = self.request('POST', '{0}/{1}/vaults/{2}/files'.format(
            self.url, self.version, vaultname))
        return resp

    def _file_url(self, vaultname=None, fileid=None, alternate_url=None,
                  blocks=False):
        """
        Return a file url
        """
        if alternate_url:
            return alternate_url
        elif blocks:
            return '{0}/{1}/vaults/{2}/files/{3}/blocks'.format(
                self.url, self.version, vaultname, fileid)
        else:
            return '{0}/{1}/vaults/{2}/files/{3}'.format(
                self.url, self.version, vaultname, fileid)

    def assign_to_file(self, blocklist_json, vaultname=None, fileid=None,
                       alternate_url=None):
        """
        Assign blocks to a file
        """

        url = self._file_url(vaultname, fileid, alternate_url)
        resp = self.request('POST', url, data=blocklist_json)
        return resp

    def finalize_file(self, filesize=None, vaultname=None, fileid=None,
                      alternate_url=None):
        """
        Finalizes a file
        """

        url = self._file_url(vaultname, fileid, alternate_url)
        parameters = {}
        if filesize is not None:
            parameters['Filesize'] = filesize
        resp = self.request('POST', url, params=parameters)
        return resp

    def list_of_blocks_in_file(self, vaultname=None, fileid=None, marker=None,
                               limit=None, alternate_url=None):
        """
        Get a list of all blocks
        """

        parameters = {}
        if marker is not None:
            parameters['marker'] = marker
        if limit is not None:
            parameters['limit'] = limit
        url = self._file_url(vaultname, fileid, alternate_url, blocks=True)
        resp = self.request('GET', url, params=parameters)
        return resp

    def get_file(self, vaultname=None, fileid=None, alternate_url=None):
        """
        Get a file
        """

        url = self._file_url(vaultname, fileid, alternate_url)
        resp = self.request('GET', url)
        return resp

    def delete_file(self, vaultname=None, fileid=None, alternate_url=None):
        """
        Delete a file
        """

        url = self._file_url(vaultname, fileid, alternate_url)
        resp = self.request('DELETE', url)
        return resp

    def list_of_files(self, vaultname=None, marker=None, limit=None,
                      alternate_url=None):
        """
        Get a list of all files
        """

        parameters = {}
        if marker is not None:
            parameters['marker'] = marker
        if limit is not None:
            parameters['limit'] = limit
        if alternate_url:
            url = alternate_url
        else:
            url = '{0}/{1}/vaults/{2}/files'.format(self.url, self.version,
                                                    vaultname)
        resp = self.request('GET', url, params=parameters)
        return resp

    def ping(self):
        """
        Ping
        """

        resp = self.request('GET', '{0}/{1}/ping'.format(self.url,
                                                         self.version))
        return resp

    def health(self):
        """
        Health
        """

        resp = self.request('GET', '{0}/{1}/health'.format(self.url,
                                                           self.version))
        return resp
