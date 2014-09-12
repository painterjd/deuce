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


config_files_root = {
    'config': '/etc/deuce/config.ini',
    'configspec': '/etc/deuce/configspec.ini'
}
config_files_user = {
    'config': '{0:}/.deuce/config.ini'.format(os.environ['HOME']),
    'configspec': '{0:}/.deuce/configspec.ini'.format(os.environ['HOME'])
}

# NOTE(TheSriram): The location of the config files can be specified here
# as of now they can be in /etc/deuce or in ~/.deuce

config_files = config_files_root

conf_ini = Config(config_files)

for k, v in config_files.items():
    if not os.path.exists(os.path.abspath(getattr(conf_ini, k))) or \
            (k + '.ini' not in getattr(conf_ini, k)):
        raise OSError("Please set absolute path to "
                      "correct {0} ini file".format(k))

configspec = ConfigObj(
    conf_ini.configspec,
    interpolation=False,
    list_values=False,
    _inspec=True)

config = ConfigObj(
    os.path.abspath(conf_ini.config),
    configspec=configspec,
    interpolation=False)
if not config.validate(Validator()):
    raise ValueError('Validation of config failed wrt to configspec')

conf = Config(config.dict())
