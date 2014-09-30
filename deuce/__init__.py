"""
Deuce Global
"""
context = None

import os
import sys
from configobj import ConfigObj
from validate import Validator

CONFIG_FILENAME = 'config.ini'
CONFIGSPEC_FILENAME = 'configspec.ini'

# This path is the path that is created when we
# have a virtual environment, or typically '/var' if
# we are installed in the system's site-packages.
CONFIG_DIR = os.path.join(sys.prefix, 'config')

spec_paths = [
    os.path.join(CONFIG_DIR, CONFIGSPEC_FILENAME),
    os.path.abspath(os.path.join("ini", CONFIGSPEC_FILENAME))
]

SPEC_PATH = None

for path in spec_paths:
    if os.path.exists(path):
        SPEC_PATH = path

if SPEC_PATH is None:  # pragma: no cover
    sys.stderr.write("Unable to find config spec. Checked here:")

    for path in spec_paths:
        sys.stderr.write("{0}\n".format(path))

    sys.exit(1)


class Config(object):
    """Builds deuce conf on passing in a dict."""
    def __init__(self, config):
        for k, v in config.items():
            if isinstance(v, dict):
                setattr(self, k, Config(v))
            else:
                setattr(self, k, v)

# List of paths where we might find the config file, in order
candidate_paths = [
    os.path.join('/etc/deuce', CONFIG_FILENAME),  # system-wide config
    os.path.join(os.environ['HOME'], ".deuce", CONFIG_FILENAME),  # homedir
    os.path.join(CONFIG_DIR, CONFIG_FILENAME),  # installed
    os.path.join(os.path.abspath("ini"), CONFIG_FILENAME)  # local dev
]


def find_config_file():
    """Checks all candidate paths looking for a config
    file. If the config file cannot be found at any
    location the process will exit with an appropriate error"""
    global candidate_paths

    chosen = None

    for candidate in candidate_paths:
        if os.path.exists(candidate):
            chosen = candidate

    if chosen is None:  # pragma: no cover

        sys.stderr.write("FATAL ERROR: could not locate a config:\n")

        for p in candidate_paths:
            sys.stderr.write("{0}\n".format(p))

        sys.exit(1)

    return chosen

configspec = ConfigObj(
    os.path.abspath(SPEC_PATH),
    interpolation=False,
    list_values=False,
    _inspec=True)

config_file = find_config_file()

config = ConfigObj(
    config_file,
    configspec=configspec,
    interpolation=False)

if not config.validate(Validator()):  # pragma: no cover
    msg = 'Validation of {0} failed using {0}'.format(
        config_file, SPEC_PATH)

    raise ValueError(msg)

conf_dict = config.dict()

conf = Config(conf_dict)
