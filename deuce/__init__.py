"""
Deuce Global
"""
context = None

import os
from configobj import ConfigObj


class Config(object):

    '''Builds deuce conf on passing in a dict.'''

    def __init__(self, config):
        for k, v in config.items():
            if isinstance(v, dict):
                setattr(self, k, Config(v))
            else:
                setattr(self, k, v)

    def __getitem__(self, k):
        return self.__dict__[k]


path_to_ini = '/etc/deuce/config.ini'
if not os.path.exists(os.path.abspath(path_to_ini)) or \
        'config.ini' not in path_to_ini:
    raise OSError("Please set absolute path to correct ini file")

config = ConfigObj(os.path.abspath(path_to_ini), interpolation=False)

conf = Config(config.dict())
