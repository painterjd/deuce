from pecan import conf
from deuce.drivers.blockstoragedriver import BlockStorageDriver

import os
import os.path
import io
import shutil

import deuce


class DiskStorageDriver(BlockStorageDriver):

    """A driver for storing blocks onto local disk

    IMPORTANT: This driver should not be considered
    secure and therefore should not be ran in
    any production environment.
    """

    def __init__(self):
        # Load the pecan config
        self._path = conf.block_storage_driver.options.path

    def _get_vault_path(self, vault_id):
        return os.path.join(self._path, str(deuce.context.project_id),
            vault_id)

    def _get_block_path(self, vault_id, block_id):
        vault_path = self._get_vault_path(vault_id)
        return os.path.join(vault_path, str(block_id))

    def create_vault(self, vault_id):
        path = self._get_vault_path(vault_id)

        if not os.path.exists(path):
            shutil.os.makedirs(path)

    def vault_exists(self, vault_id):
        path = self._get_vault_path(vault_id)
        return os.path.exists(path)

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

    def store_block(self, vault_id, block_id, blockdata):
        path = self._get_block_path(vault_id, block_id)

        with open(path, 'wb') as outfile:
            outfile.write(blockdata)

        return True

    def store_async_block(self, vault_id, block_ids, blockdatas):
        results = []
        for block_id, blockdata in zip(block_ids, blockdatas):
            path = self._get_block_path(vault_id, block_id)

            with open(path, 'wb') as outfile:
                outfile.write(blockdata)
                results.append(True)

        return True

    def block_exists(self, vault_id, block_id):
        path = self._get_block_path(vault_id, block_id)
        return os.path.exists(path)

    def delete_block(self, vault_id, block_id):
        path = self._get_block_path(vault_id, block_id)

        if os.path.exists(path):
            os.remove(path)

    def get_block_obj(self, vault_id, block_id):
        """Returns a file-like object capable or streaming the
        block data. If the object cannot be retrieved, the list
        of objects should be returned
        """
        path = self._get_block_path(vault_id, block_id)

        if not os.path.exists(path):
            return None

        return open(path, 'rb')

    def get_block_object_length(self, vault_id, block_id):
        """Returns the length of an object"""
        path = self._get_block_path(vault_id, block_id)

        if not os.path.exists(path):
            return 0

        return os.path.getsize(path)
