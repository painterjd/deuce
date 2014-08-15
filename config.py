# Server Specific Configurations
server = {
    'port': '8080',
    'host': '0.0.0.0'
}


def get_hooks():
    from deuce.hooks import DeuceContextHook
    from deuce.hooks import ProjectIDHook
    from deuce.hooks import TransactionIDHook
    from deuce.hooks import AuthTokenHook
    return [DeuceContextHook(), TransactionIDHook(), ProjectIDHook(),
        AuthTokenHook()]

# Pecan Application Configurations
app = {
    'root': 'deuce.controllers.root.RootController',
    'modules': ['deuce'],
    'static_root': '%(confdir)s/public',
    'template_path': '%(confdir)s/deuce/templates',
    'debug': True,
    'hooks': get_hooks(),
    'errors': {
        404: '/error/404',
        '__force_dict__': True
    }
}

log_directory = 'log'
import os
if not os.path.exists(log_directory):
    os.makedirs(log_directory)

logging = {
    'loggers': {
        # 'root': {'level': 'INFO', 'handlers': ['logstash']},
        # 'deuce': {'level': 'DEBUG', 'handlers': ['logstash']},
        # 'py.warnings': {'handlers': ['logstash']},
        'root': {'level': 'INFO', 'handlers': ['rotatelogfile']},
        'deuce': {'level': 'DEBUG', 'handlers': ['rotatelogfile']},
        'py.warnings': {'handlers': ['rotatelogfile']},
        '__force_dict__': True
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'simple'
        },
        'logfile': {
            'class': 'logging.FileHandler',
            'filename': os.path.join(log_directory, 'deuce.log'),
            'level': 'INFO',
            'formatter': 'simple'
        },
        'rotatelogfile': {
            'class': 'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(log_directory, 'deuce.log'),
            'level': 'INFO',
            'maxBytes': 400000000,
            'backupCount': 2,
            'formatter': 'simple'
        }  # ,
        # 'logstash': {
        #     'class': 'logstash.LogstashHandler',
        #     'level': 'INFO',
        #     'host': 'localhost',
        #     'port': 5000,
        #     'version': 1
        # }
    },
    'formatters': {
        'simple': {
            'format': ('%(asctime)s %(levelname)-5.5s [%(name)s/%(lineno)d]'
                       '[%(threadName)s] [%(request_id)s] : %(message)s')
        }
    }
}

block_storage_driver = {
    'driver': 'deuce.drivers.disk.DiskStorageDriver',
    'options': {
        'path': '/tmp/block_storage'
    },
    'swift': {
        'driver': 'deuce.drivers.swift.SwiftStorageDriver',
        'swift_module': 'swiftclient',
        'auth_url': 'Auth Url',
        'storage_url': 'Storage Url'
        # Example:
        # 'auth_url': 'https://identity.api.rackspacecloud.com/v2.0/'
    }
}

metadata_driver = {
    'driver': ('deuce.drivers.sqlite.SqliteStorageDriver'),

    'cassandra': {
        'cluster': ['127.0.0.1'],
        'keyspace': 'deucekeyspace',

        # Production DB with real cassandra
        'is_mocking': False,
        'db_module': 'cassandra.cluster',
        #
        # Mocking DB module
        # 'is_mocking': True,
        # 'db_module': 'deuce.tests.mock_cassandra',
    },

    'sqlite': {
        'path': ':memory:',
        'db_module': 'sqlite3'
    },

    'mongodb': {
        'path': 'deuce_mongo_unittest_vaultmeta',
        'url': 'mongodb://127.0.0.1',
        'db_file': '/tmp/deuce_mongo_unittest_vaultmeta.db',

        # Production DB module or unittest
        # With real mongodb daemon.
        'db_module': 'pymongo',
        #
        # Mocking DB module.
        # 'db_module': 'deuce.tests.db_mocking.mongodb_mocking',

        # An arbitary segment number for blocks fetching and
        # transferring from fileblocks collection to file collection
        'FileBlockReadSegNum': 1000,
        # 'FileBlockReadSegNum': 10,

        # pymongo block number in each File document
        'maxFileBlockSegNum': 100000
        # 'maxFileBlockSegNum': 30
    }
}

api_configuration = {
    # Define system limitation on page size
    'max_returned_num': 80
}
