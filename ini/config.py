# Server Specific Configurations
import os
from configobj import ConfigObj

# NOTE(TheSriram): Please reset path_to_ini to a
# fully qualified path pointing to config.ini

path_to_ini = '/random_path/config.ini'
if not os.path.exists(os.path.abspath(path_to_ini)) or \
        'config.ini' not in path_to_ini:
    raise OSError("Please set absolute path to correct ini file")
config = ConfigObj(os.path.abspath(path_to_ini), interpolation=False)

server = {
    'port': config['server']['port'],
    'host': config['server']['host']
}


def get_hooks():
    from deuce.hooks import DeuceContextHook
    from deuce.hooks import ProjectIDHook
    from deuce.hooks import TransactionIDHook
    from deuce.hooks import OpenStackHook
    return [DeuceContextHook(), TransactionIDHook(), ProjectIDHook(),
            OpenStackHook()]

# Pecan Application Configurations
app = {
    'root': config['app']['root'],
    'modules': [config['app']['modules']],
    'debug': bool(config['app']['debug']),
    'hooks': get_hooks(),
    'errors': {
        404: config['app']['errors']['404'],
        '__force_dict__': bool(config['app']['errors']['__force_dict__'])
    }
}

log_directory = config['logging']['log_directory']
if not os.path.exists(log_directory):  # pragma: no cover
    os.makedirs(log_directory)

logging = {
    'loggers': {
        'root': {'level': config['loggers']['root']['level'],
                 'handlers': [config['loggers']['root']['handlers']]},
        'deuce': {'level': config['loggers']['deuce']['level'],
                  'handlers': [config['loggers']['deuce']['handlers']]},
        'py.warnings': {'handlers': [config['loggers']['py.warnings']
                                     ['handlers']]},
        '__force_dict__': config['loggers']['__force_dict__']
    },
    'handlers': {
        'console': {
            'level': config['handlers']['console']['level'],
            'class': config['handlers']['console']['class'],
            'formatter': config['handlers']['console']['formatter']
        },
        'logfile': {
            'class': config['handlers']['logfile']['class'],
            'filename': os.path.join(log_directory,
                                     config['handlers']['logfile']
                                     ['filename']),
            'level': config['handlers']['logfile']['level'],
            'formatter': config['handlers']['logfile']['formatter']
        },
        'syslog': {
            #  'socktype': config['handlers']['syslog']['socktype'],
            #  'facility': config['handlers']['syslog']['facility'],
            'class': config['handlers']['syslog']['class'],
            'level': config['handlers']['syslog']['level'],
            'formatter': config['handlers']['syslog']['formatter'],
            'address': config['handlers']['syslog']['address']
        },
        'rotatelogfile': {
            'class': config['handlers']['rotatelogfile']['class'],
            'filename': os.path.join(log_directory,
                                     config['handlers']['rotatelogfile']
                                     ['filename']),
            'level': config['handlers']['rotatelogfile']['level'],
            'maxBytes': int(config['handlers']['rotatelogfile']['maxBytes']),
            'backupCount': int(config['handlers']['rotatelogfile']
                               ['backupCount']),
            'formatter': config['handlers']['rotatelogfile']['formatter']
        },
        'logstash': {
            'class': config['handlers']['logstash']['class'],
            'level': config['handlers']['logstash']['level'],
            'host': config['handlers']['logstash']['host'],
            'port': int(config['handlers']['logstash']['port']),
            'version': int(config['handlers']['logstash']['version'])
        }
    },
    'formatters': {
        'standard': {
            'format': (config['formatters']['standard']['format'])
        }
    }
}

block_storage_driver = {
    'driver': config['block_storage_driver']['driver'],
    'options': {
        'path': config['block_storage_driver']['options']['path']
    },
    'swift': {

        'driver': config['block_storage_driver']['swift']['driver'],
        'swift_module': config['block_storage_driver']['swift']
                              ['swift_module'],

        'testing': {
            'is_mocking': bool(config['block_storage_driver']['swift']
                               ['testing']['is_mocking']),
            'username': config['block_storage_driver']['swift']['testing']
                              ['username'],
            'password': config['block_storage_driver']['swift']['testing']
                              ['password'],
            'auth_url': config['block_storage_driver']['swift']['testing']
                              ['auth_url'],
            'storage_url': config['block_storage_driver']['swift']['testing']
                                 ['storage_url']
            # Example:
            # 'auth_url': 'https://identity.api.rackspacecloud.com/v2.0/'
        },

    }
}

metadata_driver = {
    'driver': (config['metadata_driver']['driver']),

    'cassandra': {
        'cluster': [config['metadata_driver']['cassandra']['cluster']],
        'keyspace': config['metadata_driver']['cassandra']['keyspace'],
        'db_module': config['metadata_driver']['cassandra']['db_module'],

        # Testing configuration
        'testing': {
            # Mocking DB module
            'is_mocking': config['metadata_driver']['cassandra']['testing']
                                ['is_mocking']
        }
    },

    'sqlite': {
        'path': config['metadata_driver']['sqlite']['path'],
        'db_module': config['metadata_driver']['sqlite']['db_module']
    },

    'mongodb': {
        'path': config['metadata_driver']['mongodb']['path'],
        'url': config['metadata_driver']['mongodb']['url'],
        'db_file': config['metadata_driver']['mongodb']['db_file'],

        # Production DB module or unittest
        # With real mongodb daemon.
        'db_module': config['metadata_driver']['mongodb']['db_module'],
        #
        # Mocking DB module.
        # 'db_module': 'deuce.tests.db_mocking.mongodb_mocking',

        # An arbitary segment number for blocks fetching and
        # transferring from fileblocks collection to file collection
        'FileBlockReadSegNum': config['metadata_driver']['mongodb']
                                     ['FileBlockReadSegNum'],
        # 'FileBlockReadSegNum': 10,

        # pymongo block number in each File document
        'maxFileBlockSegNum': config['metadata_driver']['mongodb']
                                    ['maxFileBlockSegNum'],
        # 'maxFileBlockSegNum': 30

        'testing': {
            'is_mocking': config['metadata_driver']['mongodb']
                                ['testing']
        }
    }
}

api_configuration = {
    # Define system limitation on page size
    'max_returned_num': config['api_configuration']['max_returned_num']
}
