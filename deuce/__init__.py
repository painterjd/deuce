"""
Deuce Global
"""
context = None

import os
from configobj import ConfigObj
from validate import Validator


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
configspecfilename = '/etc/deuce/configspec.ini'

configspec = ConfigObj(
    configspecfilename,
    interpolation=False,
    list_values=False,
    _inspec=True)
if not os.path.exists(os.path.abspath(path_to_ini)) or \
        'config.ini' not in path_to_ini:
    raise OSError("Please set absolute path to correct ini file")

config = ConfigObj(
    os.path.abspath(path_to_ini),
    configspec=configspec,
    interpolation=False)
if not config.validate(Validator()):
    raise ValueError('Validation of config failed wrt to configspec')

conf = Config(config.dict())
