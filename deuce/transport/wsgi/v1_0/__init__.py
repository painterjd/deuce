from deuce.transport.wsgi.v1_0 import blocks
from deuce.transport.wsgi.v1_0 import blockstorage
from deuce.transport.wsgi.v1_0 import fileblocks
from deuce.transport.wsgi.v1_0 import files
from deuce.transport.wsgi.v1_0 import health
from deuce.transport.wsgi.v1_0 import home
from deuce.transport.wsgi.v1_0 import ping
from deuce.transport.wsgi.v1_0 import vault


def public_endpoints():

    return [
        # Home
        ('/',
        home.Resource()),

        ('/health',
        health.CollectionResource()),

        ('/ping',
        ping.CollectionResource()),
        # Vault Endpoints
        ('/vaults/{vault_id}',
         vault.ItemResource()),

        ('/vaults',
         vault.CollectionResource()),

        # Block Endpoints
        ('/vaults/{vault_id}/blocks',
         blocks.CollectionResource()),
        ('/vaults/{vault_id}/blocks/{block_id}',
         blocks.ItemResource()),

        # File Endpoints
        ('/vaults/{vault_id}/files',
         files.CollectionResource()),

        ('/vaults/{vault_id}/files/{file_id}',
         files.ItemResource()),

        # FileBlock Endpoints
        ('/vaults/{vault_id}/files/{file_id}/blocks',
         fileblocks.CollectionResource()),

        # Block Storage Endpoints
        ('/vaults/{vault_id}/storage/blocks',
         blockstorage.CollectionResource()),
        ('/vaults/{vault_id}/storage/blocks/{storage_block_id}',
         blockstorage.ItemResource()),
    ]
