from pecan import conf

# Hoist up stuff into the model namespace
from deuce.model.vault import Vault
from deuce.model.block import Block
from deuce.model.file import File
from deuce.model.health import Health

# Load the storage drivers manually into the model. Note:
# This should change significantly.
from deuce.drivers.disk import DiskStorageDriver

import deuce
import importlib

deuce.storage_driver = None
deuce.metadata_driver = None


def _load_driver(classname):
    """Creates of the instance of the specified
    class given the fully-qualified name. The module
    is dynamically imported.
    """
    pos = classname.rfind('.')
    module_name = classname[:pos]
    class_name = classname[pos + 1:]

    mod = importlib.import_module(module_name)
    return getattr(mod, class_name)()


def init_model():
    # Load metadata driver
    deuce.metadata_driver = _load_driver(conf.metadata_driver.driver)
    deuce.storage_driver = _load_driver(conf.block_storage_driver.driver)
