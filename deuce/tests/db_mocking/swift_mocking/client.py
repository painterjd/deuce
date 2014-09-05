

import os
import io
import shutil
from swiftclient.exceptions import ClientException
import hashlib
import uuid

import datetime
import atexit

container_path = '/tmp/swift_mocking'
mock_drop_connection_status = False


def mock_drop_connections(drop_status):
    global mock_drop_connection_status
    mock_drop_connection_status = drop_status


def _clean_up_mocking():
    import os.path

    if os.path.exists(container_path):
        shutil.rmtree(container_path)

atexit.register(_clean_up_mocking)


def _get_vault_path(vault_id):
    return os.path.join(container_path, vault_id)


def _get_vault_block_path(vault_id):
    vault_path = _get_vault_path(vault_id)
    return os.path.join(vault_path, 'blocks')


def _get_block_path(vault_id, block_id):
    if block_id.startswith('blocks/'):
        vault_path = _get_vault_path(vault_id)
        return os.path.join(vault_path, str(block_id))
    else:
        vault_block_path = _get_vault_block_path(vault_id)
        return os.path.join(vault_block_path, str(block_id))


# Create Vault
def put_container(url,
            token,
            container,
            response_dict):
    if mock_drop_connection_status:
        raise ClientException('mocking network drop')

    path = _get_vault_path(container)
    if not os.path.exists(path):
        shutil.os.makedirs(path)
    else:
        raise ClientException('mocking')

    block_path = _get_vault_block_path(container)
    if not os.path.exists(block_path):
        shutil.os.makedirs(block_path)
    response_dict['status'] = 201


# Check Vault
def head_container(url,
            token,
            container):
    if mock_drop_connection_status:
        raise ClientException('mocking network drop')

    path = _get_vault_path(container)
    if os.path.exists(path):
        response_headers = {}
        response_headers['content-type'] = 'mocking_ret'
        response_headers['x-container-bytes-used'] = 0
        response_headers['x-container-object-count'] = 0
        # If we really wanted to be pendantic about this field
        # then we'd set this to zero and find the epoch to 5 decimals
        # on each file below and take the latest (max) value between
        # them all
        response_headers['x-timestamp'] = 987654321.12345

        total_size = 0
        object_count = 0
        for root, dirs, files in os.walk(path):
            total_size = total_size + sum(
                os.path.getsize(
                    os.path.join(root, name)) for name in files)
            object_count = object_count + len(files)

        response_headers['x-container-bytes-used'] = total_size
        response_headers['x-container-object-count'] = object_count

        return response_headers
    else:
        raise ClientException('mocking')


# Delete Vault
def delete_container(url,
            token,
            container,
            response_dict):
    if mock_drop_connection_status:
        raise ClientException('mocking network drop')

    try:
        # Basic response requirements
        response_dict['content-length'] = 0
        response_dict['content-type'] = 'text/html; charset=UTF-8'
        response_dict['x-transaction-id'] = 'req-' + str(uuid.uuid4())
        # Thu, 16 Jan 2014 18:04:04 GMT
        response_dict['date'] = datetime.datetime.utcnow().strftime(
            "%a, %d %b %Y %H:%M:%S %Z")

        path = _get_vault_path(container)
        blockpath = _get_vault_block_path(container)

        response_dict['x-vault-path'] = path
        response_dict['x-block-path'] = blockpath

        if os.path.exists(path):

            if os.path.exists(blockpath):
                if os.listdir(blockpath) != []:
                    # container not empty
                    response_dict['x-error-message'] = 'container not empty'
                    response_dict['status'] = 409
                else:
                    # no blocks we're okay

                    # container exists and is empty (no blocks)
                    shutil.rmtree(path)

                    response_dict['status'] = 204

            elif os.listdir(path) == []:
                # container exists and is empty (no blocks)
                shutil.rmtree(path)

                response_dict['status'] = 204

            else:
                # else there is some other issue
                response_dict['x-error-message'] = 'mocking: listing directory'
                response_dict['status'] = 500
                assert False

        else:
            # Container does not exist
            response_dict['x-error-message'] = 'vault does not exist'
            response_dict['status'] = 404

    except Exception as ex:
        response_dict['x-error-message'] = 'mocking error: {0:}'.format(ex)
        response_dict['status'] = 500
        raise ClientException('mocking error: {0:}'.format(ex))


# Store Block
def put_object(url,
            token,
            container,
            name,
            contents,
            content_length,
            response_dict,
            etag=None):
    if mock_drop_connection_status:
        raise ClientException('mocking network drop')

    blocks_path = _get_vault_block_path(container)
    if not os.path.exists(blocks_path):
        raise ClientException('mocking')

    path = _get_block_path(container, name)

    with open(path, 'wb') as outfile:
        outfile.write(contents)

    mdhash = hashlib.md5()
    mdhash.update(contents)
    response_dict['status'] = 201
    return mdhash.hexdigest()


# Store multiple blocks
def put_async_object(url,
                     token,
                     container,
                     names,
                     contents,
                     response_dict,
                     etag=True):
    etags = []
    if mock_drop_connection_status:
        raise ClientException('mocking network drop')

    for name, content in zip(names, contents):
        blocks_path = _get_vault_path(container)
        if not os.path.exists(blocks_path):
            raise ClientException('mocking')

        path = _get_block_path(container, name)

        with open(path, 'wb') as outfile:
            outfile.write(content)

        mdhash = hashlib.md5()
        etags.append(mdhash.update(content))
    response_dict['status'] = 201
    return etags


# Check Block
def head_object(url,
            token,
            container,
            name):
    if mock_drop_connection_status:
        raise ClientException('mocking network drop')

    path = _get_block_path(container, name)
    if not os.path.exists(path):
        raise ClientException('mocking')
    return 'mocking_ret'


# Delete Block
def delete_object(url,
            token,
            container,
            name,
            response_dict):
    if mock_drop_connection_status:
        raise ClientException('mocking network drop')

    path = _get_block_path(container, name)
    if os.path.exists(path):
        os.remove(path)
        response_dict['status'] = 201
    else:
        raise ClientException('mocking')


# Get Block
def get_object(url,
            token,
            container,
            name,
            response_dict):
    if mock_drop_connection_status:
        raise ClientException('mocking network drop')

    path = _get_block_path(container, name)

    if not os.path.exists(path):
        raise ClientException('mocking')

    buff = ""
    with open(path, 'rb') as infile:
        buff = infile.read()

    mdhash = hashlib.md5()
    mdhash.update(buff)
    etag = mdhash.hexdigest()

    hdrs = {}
    hdrs['content-length'] = os.path.getsize(path)
    hdrs['last-modified'] = os.path.getmtime(path)
    hdrs['accept-ranges'] = 'bytes'
    hdrs['etag'] = etag
    return hdrs, buff


def get_keystoneclient_2_0(auth_url,
            user,
            key,
            os_options):
    if user == 'failing_auth_hook':
        raise ClientException('mocking auth failure')
    return 'mocking_project_id', 'mocking_project_token'
