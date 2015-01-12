import io
import os
import os.path
import shutil

import deuce
from deuce import conf
from deuce.drivers.blockstoragedriver import BlockStorageDriver
from deuce.util import log


logger = log.getLogger(__name__)


class DiskStorageDriver(BlockStorageDriver):

    """A driver for storing blocks onto local disk

    IMPORTANT: This driver should not be considered
    secure and therefore should not be ran in
    any production environment.
    """

    vault_permission = 0o750
    block_permission = 0o640

    def __init__(self):
        self._path = conf.block_storage_driver.options.path

    def _get_project_path(self):
        return os.path.join(self._path, str(deuce.context.project_id))

    def _get_vault_path(self, vault_id):
        return os.path.join(self._get_project_path(), vault_id)

    def _get_block_path(self, vault_id, storage_block_id):
        vault_path = self._get_vault_path(vault_id)
        return os.path.join(vault_path, str(storage_block_id))

    def create_vault(self, vault_id):
        path = self._get_vault_path(vault_id)

        if not os.path.exists(path):
            shutil.os.makedirs(path)
            os.chmod(self._get_project_path(),
                     DiskStorageDriver.vault_permission)
            os.chmod(path, DiskStorageDriver.vault_permission)

    def vault_exists(self, vault_id):
        path = self._get_vault_path(vault_id)
        return os.path.exists(path)

    def get_vault_block_list(self, vault_id, limit, marker=None):

        path = self._get_vault_path(vault_id)
        if os.path.exists(path):
            total_contents = os.listdir(path)
            total_contents.sort()
            if marker:
                try:
                    index = total_contents.index(marker)
                    return total_contents[index:(index + limit)]
                except ValueError:
                    return []
            else:
                return total_contents[:limit]

        else:
            return None

    def get_vault_statistics(self, vault_id):
        """Return the statistics on the vault.

        "param vault_id: The ID of the vault to gather statistics for"""

        statistics = dict()
        statistics['internal'] = {}
        statistics['total-size'] = 0
        statistics['block-count'] = 0

        path = self._get_vault_path(vault_id)

        total_size = 0
        object_count = 0
        for root, dirs, files in os.walk(path):
            total_size = total_size + sum(
                os.path.getsize(
                    os.path.join(root, name)) for name in files)
            object_count = object_count + len(files)

        statistics['total-size'] = total_size
        statistics['block-count'] = object_count

        return statistics

    def delete_vault(self, vault_id):
        path = self._get_vault_path(vault_id)
        try:
            if os.path.exists(path):

                if os.listdir(path) == []:
                    # There's nothing in the vault.
                    # It's safe to delete
                    shutil.rmtree(path)
                    return True

                else:
                    # There's data there
                    return False

            else:
                # Vault doesn't exist, so it's already been deleted
                return True

        except:  # pragma: no cover
            # An error occurred
            return False

    def store_block(self, vault_id, metadata_block_id, blockdata):
        storage_id = self.storage_id(metadata_block_id)
        path = self._get_block_path(vault_id, storage_id)

        returnValue = False
        returnStorageId = ''
        outfile = None

        try:
            # (BenjamenMeyer) - Using a open() in a context will
            # oddly result in the exiting of the context being
            # not covered even though the success and failure
            # paths can be proven to be covered.
            outfile = open(path, 'wb')
            outfile.write(blockdata)
            outfile.flush()

            returnValue = True
            returnStorageId = storage_id

        except:
            returnValue = False
            returnStorageId = ''

        finally:  # pragma: no cover
            if outfile is not None:
                if not outfile.closed:
                    outfile.close()
                os.chmod(path, DiskStorageDriver.block_permission)

        return (returnValue, returnStorageId)

    def store_async_block(self, vault_id, metadata_block_ids, blockdatas):
        storage_ids = [self.storage_id(metadata_block_id)
                       for metadata_block_id in metadata_block_ids]
        try:
            for storage_id, blockdata in zip(storage_ids, blockdatas):
                path = self._get_block_path(vault_id, storage_id)

                # (BenjamenMeyer) - Using a open() in a context will
                # oddly result in the exiting of the context being
                # not covered even though the success and failure
                # paths can be proven to be covered.
                try:
                    outfile = open(path, 'wb')
                    outfile.write(blockdata)
                    outfile.flush()

                except:
                    pass

                finally:
                    if outfile is not None:  # pragma: no cover
                        if not outfile.closed:
                            outfile.close()
                        os.chmod(path, DiskStorageDriver.block_permission)

            return (True, storage_ids)

        except:
            return (False, [])

    def block_exists(self, vault_id, storage_block_id):
        path = self._get_block_path(vault_id, storage_block_id)
        return os.path.exists(path)

    def delete_block(self, vault_id, storage_block_id):
        path = self._get_block_path(vault_id, storage_block_id)

        if os.path.exists(path):
            os.remove(path)
            return True
        else:
            return False

    def get_block_obj(self, vault_id, storage_block_id):
        """Returns a file-like object capable or streaming the
        block data. If the object cannot be retrieved, the list
        of objects should be returned
        """
        path = self._get_block_path(vault_id, storage_block_id)

        if not os.path.exists(path):
            return None

        return open(path, 'rb')

    def get_block_object_length(self, vault_id, storage_block_id):
        """Returns the length of an object"""
        path = self._get_block_path(vault_id, storage_block_id)

        if not os.path.exists(path):
            return 0

        return os.path.getsize(path)
