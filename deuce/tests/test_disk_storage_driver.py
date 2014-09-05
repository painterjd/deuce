import os
from deuce.tests import FunctionalTest, DummyContextObject
from deuce.drivers.blockstoragedriver import BlockStorageDriver
from deuce.drivers.disk import DiskStorageDriver
from deuce.tests.util import MockFile

from hashlib import md5

# TODO: Make this test generic -- it should not konw
# which particular driver it is testing.


class DiskStorageDriverTest(FunctionalTest):

    def create_driver(self):
        return DiskStorageDriver()

    def get_Auth_Token(self):
        return None, None

    def test_ancestry(self):
        driver = self.create_driver()

        assert isinstance(driver, BlockStorageDriver)
        assert isinstance(driver, object)

    def test_basic_construction(self):
        driver = self.create_driver()

    def test_vault_crud(self):
        driver = self.create_driver()

        vault_id = self.create_vault_id()

        driver.delete_vault(vault_id)
        assert not driver.vault_exists(vault_id)

        # delete a non-empty vault.
        driver.create_vault(vault_id)
        block_id = 'baab'
        driver.store_block(vault_id, block_id, b' ')
        assert driver.block_exists(vault_id, block_id)
        assert not driver.delete_vault(vault_id)
        assert driver.vault_exists(vault_id)
        # Cleanup and delete again.
        driver.delete_block(vault_id, block_id)
        assert driver.delete_vault(vault_id)
        assert not driver.vault_exists(vault_id)

        # To create an existed vault.
        driver.create_vault(vault_id)
        driver.create_vault(vault_id)

        assert driver.vault_exists(vault_id)

        driver.delete_vault(vault_id)

        assert not driver.vault_exists(vault_id)

    def test_vault_statistics(self):
        driver = self.create_driver()

        vault_id = 'vault_id'

        # empty vault stats
        driver.create_vault(vault_id)

        statistics = driver.get_vault_statistics(vault_id)

        main_keys = ('total-size', 'block-count')
        for key in main_keys:
            assert key in statistics.keys()
            assert statistics[key] == 0

    def test_block_crud(self):
        driver = self.create_driver()

        block_size = 3000
        vault_id = self.create_vault_id()

        driver.create_vault(vault_id)

        # Create a file-like object
        block_data = MockFile(block_size)

        # Test Invalid block_id, ie, wrong sha1 hash.
        try:
            driver.store_block(vault_id, "test_disk_trouble_file",
                os.urandom(10))
        except:
            assert True
        driver.delete_block(vault_id, "test_disk_trouble_file")

        assert (driver.get_block_object_length(vault_id,
            "test_invalid_block_for_length") == 0)

        # Test delete invalid block
        driver.delete_block(vault_id, "test_invalid_block_deletion")

        # Test valid block_id.
        block_id = block_data.sha1()
        driver.store_block(vault_id, block_id, block_data.read())
        block_data.seek(0)

        assert driver.block_exists(vault_id, block_id)

        # Read back the block data and compare
        file_obj = driver.get_block_obj(vault_id, block_id)

        returned_data = file_obj.read()

        # Returned data should be exatly the same

        assert len(returned_data) == block_size
        assert returned_data == block_data._content
        assert (driver.get_block_object_length(vault_id, block_id)
            == block_size)

        driver.delete_block(vault_id, block_id)

        assert not driver.block_exists(vault_id, block_id)

        assert None == driver.get_block_obj(vault_id, 'invalid_block_id')

        assert driver.delete_vault(vault_id)

    def test_multi_block_crud(self):
        storage_url, token = self.get_Auth_Token()

        driver = self.create_driver()

        block_size = 3000
        vault_id = 'multi_block_crud_vault_test'
        projectid = 'multi_block_test_project_id'

        driver.create_vault(vault_id)
        block_datas = [MockFile(block_size) for _ in range(3)]
        block_ids = [block_data.sha1() for block_data in block_datas]
        driver.store_async_block(vault_id, block_ids, [
            block_data.read() for block_data in block_datas])
        for block_id, block_data in zip(block_ids, block_datas):
            assert driver.block_exists(vault_id, block_id)

            # Read back the block data and compare
            file_obj = driver.get_block_obj(vault_id, block_id)

            returned_data = file_obj.read()

            # Returned data should be exatly the same

            assert len(returned_data) == block_size
            assert returned_data == block_data._content

            driver.delete_block(vault_id, block_id)

            assert not driver.block_exists(vault_id, block_id)

            assert None == driver.get_block_obj(vault_id, 'invalid_block_id')
        assert driver.delete_vault(vault_id)

    def test_block_generator(self):
        driver = self.create_driver()

        block_size = 3000
        vault_id = self.create_vault_id()

        driver.create_vault(vault_id)

        # Test re-entrance
        driver.create_vault(vault_id)

        blocks = [MockFile(block_size) for x in range(0, 10)]

        orig_hash = md5()

        for block_data in blocks:
            orig_hash.update(block_data._content)

        orig_hex = orig_hash.hexdigest()

        block_ids = []
        for block_data in blocks:
            block_id = block_data.sha1()
            block_ids.append(block_id)
            driver.store_block(vault_id, block_id,
                block_data.read())
            block_data.seek(0)

        # Now call the block generator.

        blockid_gen = block_ids[:]

        gen = driver.create_blocks_generator(vault_id, blockid_gen)

        fetched_data = list(gen)

        assert len(fetched_data) == len(blocks) == 10

        for x in range(0, len(fetched_data)):
            blocks[x].seek(0)
            assert fetched_data[x].read() == blocks[x].read()

        # Clenaup.
        for block_id in block_ids[:]:
            driver.delete_block(vault_id, block_id)
        assert driver.delete_vault(vault_id)
