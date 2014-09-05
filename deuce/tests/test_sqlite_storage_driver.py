import os
from deuce.tests import FunctionalTest
from deuce.drivers.metadatadriver import MetadataStorageDriver, GapError,\
    OverlapError, ConstraintError
from deuce.drivers.sqlite import SqliteStorageDriver
import random

from mock import MagicMock


class SqliteStorageDriverTest(FunctionalTest):

    def create_driver(self):
        return SqliteStorageDriver()

    def test_basic_construction(self):
        driver = SqliteStorageDriver()

    def test_geneology(self):
        driver = SqliteStorageDriver()
        assert isinstance(driver, MetadataStorageDriver)
        assert isinstance(driver, object)

    def test_vault_statistics(self):
        driver = self.create_driver()

        hdr_data = {
            'x-project-id': self.create_project_id(),
            'x-auth-token': self.create_auth_token()
        }
        self.init_context(hdr_data)
        vault_id = self.create_vault_id()

        # empty vault stats
        # TODO ** Create Vault Here **
        statistics = driver.get_vault_statistics(vault_id)

        main_keys = ('files', 'blocks')
        for key in main_keys:
            assert key in statistics.keys()
            assert 'count' in statistics[key].keys()
            assert statistics[key]['count'] == 0

        # TODO: Add files and check that founds match as expected

    def test_db_health(self):
        driver = self.create_driver()
        retval = driver.get_health()
        driver.get_health = MagicMock(return_value=str('is not active.'))
        retval = driver.get_health()
        assert retval == str('is not active.')

    def test_file_crud(self):
        driver = self.create_driver()

        hdr_data = {
            'x-project-id': self.create_project_id(),
            'x-auth-token': self.create_auth_token()
        }
        self.init_context(hdr_data)

        vault_id = self.create_vault_id()
        file_id = self.create_file_id()

        assert not driver.has_file(vault_id, file_id)

        # Length of Non-existent file is 0
        file_length = driver.file_length(vault_id, file_id)
        assert (file_length == 0)

        driver.create_file(vault_id, file_id)

        assert driver.has_file(vault_id, file_id)
        file_length = driver.file_length(vault_id, file_id)
        assert (file_length == 0)

        data = driver.get_file_data(vault_id, file_id)

        driver.delete_file(vault_id, file_id)

        assert not driver.has_file(vault_id, file_id)

    def test_finalize_empty_file(self):
        driver = self.create_driver()

        hdr_data = {
            'x-project-id': self.create_project_id(),
            'x-auth-token': self.create_auth_token()
        }
        self.init_context(hdr_data)

        vault_id = self.create_vault_id()
        file_id = self.create_file_id()

        driver.create_file(vault_id, file_id)

        assert not driver.is_finalized(vault_id, file_id)

        driver.finalize_file(vault_id, file_id)

        assert driver.is_finalized(vault_id, file_id)
        file_length = driver.file_length(vault_id, file_id)
        assert (file_length == 0)

    def test_finalize_nonexistent_file(self):
        driver = self.create_driver()

        hdr_data = {
            'x-project-id': self.create_project_id(),
            'x-auth-token': self.create_auth_token()
        }
        self.init_context(hdr_data)

        vault_id = self.create_vault_id()
        file_id = self.create_file_id()

        assert not driver.has_file(vault_id, file_id)
        retval = driver.finalize_file(vault_id, file_id)

        file_length = driver.file_length(vault_id, file_id)
        assert (file_length == 0)

        try:
            data = driver.get_file_data(vault_id, file_id)
        except:
            assert True

        assert not driver.has_file(vault_id, file_id)
        assert not driver.is_finalized(vault_id, file_id)

    def test_block_crud(self):
        driver = self.create_driver()

        hdr_data = {
            'x-project-id': self.create_project_id(),
            'x-auth-token': self.create_auth_token()
        }
        self.init_context(hdr_data)

        vault_id = self.create_vault_id()
        block_id = self.create_block_id()
        size = 4096

        assert not driver.has_block(vault_id, block_id)
        try:
            size = driver.get_block_data(vault_id, block_id)['blocksize']
        except:
            assert True
        driver.register_block(vault_id, block_id, size)

        assert driver.has_block(vault_id, block_id)

        self.assertEqual(driver.get_block_data(vault_id,
                                               block_id)['blocksize'], size)

        # Call again, shouldn't throw
        driver.register_block(vault_id, block_id, size)

        driver.unregister_block(vault_id, block_id)
        assert not driver.has_block(vault_id, block_id)

        assert not driver.has_block(vault_id, 'invalidid')

    def test_file_assignment_no_block(self):

        driver = self.create_driver()

        hdr_data = {
            'x-project-id': self.create_project_id(),
            'x-auth-token': self.create_auth_token()
        }
        self.init_context(hdr_data)

        vault_id = self.create_vault_id()
        file_id = self.create_file_id()

        self.assertEqual(driver.has_file(vault_id,
                                         file_id), False)

        driver.create_file(vault_id, file_id)

        self.assertEqual(driver.has_file(vault_id, file_id), True)

        driver.assign_block(vault_id, file_id, 'block_a', 0)
        driver.assign_block(vault_id, file_id, 'block_b', 1024)

        with self.assertRaises(GapError) as ctx:
            driver.finalize_file(vault_id, file_id, 2048)

        self.assertEqual(ctx.exception.vault_id, vault_id)
        self.assertEqual(ctx.exception.file_id, file_id)
        self.assertEqual(ctx.exception.startpos, 0)
        self.assertEqual(ctx.exception.endpos, 2048)

        self.assertEqual(driver.is_finalized(vault_id, file_id),
                         False)

    def test_delete_block_no_refs(self):
        driver = self.create_driver()

        hdr_data = {
            'x-project-id': self.create_project_id(),
            'x-auth-token': ''
        }
        self.init_context(hdr_data)

        vault_id = self.create_vault_id()

        block_id = 'block_0'
        block_size = 1024

        self.assertEqual(driver.get_block_ref_count(vault_id, block_id), 0)

        driver.register_block(vault_id, block_id, block_size)

        self.assertEqual(driver.get_block_ref_count(vault_id, block_id), 0)

        driver.unregister_block(vault_id, block_id)

        self.assertEqual(driver.get_block_ref_count(vault_id, block_id), 0)

    def test_delete_block_with_refs(self):
        driver = self.create_driver()

        hdr_data = {
            'x-project-id': self.create_project_id(),
            'x-auth-token': ''
        }
        self.init_context(hdr_data)

        vault_id = self.create_vault_id()
        file_id = self.create_file_id()

        block_id = 'block_0'
        block_size = 1024

        driver.create_file(vault_id, file_id)

        driver.register_block(vault_id, block_id, block_size)
        self.assertTrue(driver.has_block(vault_id, block_id))

        # Should have refs until assigned, then it should have three
        self.assertEqual(driver.get_block_ref_count(vault_id, block_id), 0)
        driver.assign_block(vault_id, file_id, block_id, 0)
        driver.assign_block(vault_id, file_id, block_id, 1024)
        self.assertEqual(driver.get_block_ref_count(vault_id, block_id), 2)

        # Now test unregistering the block. This should fail
        with self.assertRaises(ConstraintError) as ctx:
            driver.unregister_block(vault_id, block_id)

        self.assertEqual(ctx.exception.project_id, hdr_data['x-project-id'])
        self.assertEqual(ctx.exception.vault_id, vault_id)

    def test_delete_file_check_refs(self):
        driver = self.create_driver()

        hdr_data = {
            'x-project-id': self.create_project_id(),
            'x-auth-token': ''
        }
        self.init_context(hdr_data)

        vault_id = self.create_vault_id()
        file_id = self.create_file_id()
        block_id = 'block_0'
        block_size = 1024

        driver.create_file(vault_id, file_id)
        driver.register_block(vault_id, block_id, block_size)
        driver.assign_block(vault_id, file_id, block_id, 0)

        self.assertEqual(driver.get_block_ref_count(vault_id, block_id), 1)
        driver.delete_file(vault_id, file_id)
        self.assertEqual(driver.get_block_ref_count(vault_id, block_id), 0)

    def test_block_references(self):

        driver = self.create_driver()

        hdr_data = {
            'x-project-id': self.create_project_id(),
            'x-auth-token': ''
        }
        self.init_context(hdr_data)

        vault_id = self.create_vault_id()

        num_elements = 1

        # Create a bunch of file IDs
        file_ids = [self.create_file_id() for _ in range(0, num_elements)]

        # Now create some file IDs
        block_ids = ['block_{0}'.format(x) for x in range(0, num_elements)]

        block_size = 1024

        # Create each of those files

        for file_id in file_ids:
            driver.create_file(vault_id, file_id)

        for block_id in block_ids:
            self.assertEqual(driver.has_block(vault_id, block_id), False)

            # Check the block references on a non-existent block. Should be 0
            self.assertEqual(driver.get_block_ref_count(vault_id, block_id), 0)

            driver.register_block(vault_id, block_id, block_size)
            self.assertEqual(driver.has_block(vault_id, block_id), True)

            # Check the block references for these blocks. They should all be 0
            self.assertEqual(driver.get_block_ref_count(vault_id, block_id), 0)

        # Now assign each block to a single file. The reference count for each
        # block should be incremented to 1
        for i in range(0, num_elements):
            file_id = file_ids[i]
            block_id = block_ids[i]

            self.assertEqual(driver.get_block_ref_count(vault_id, block_id), 0)
            driver.assign_block(vault_id, file_id, block_id, 0)
            self.assertEqual(driver.get_block_ref_count(vault_id, block_id), 1)

            # Each 'file' is one block in length
            driver.finalize_file(vault_id, file_id, file_size=block_size)

            self.assertEqual(driver.is_finalized(vault_id, file_id), True)

            # Finalizing the file should not change the block
            # reference count
            self.assertEqual(driver.get_block_ref_count(vault_id, block_id), 1)

    def test_file_assignment_registration(self):

        driver = self.create_driver()

        hdr_data = {
            'x-project-id': self.create_project_id(),
            'x-auth-token': self.create_auth_token()
        }
        self.init_context(hdr_data)

        vault_id = self.create_vault_id()
        file_id = self.create_file_id()

        self.assertEqual(driver.has_file(vault_id, file_id), False)

        driver.create_file(vault_id, file_id)

        self.assertEqual(driver.has_file(vault_id, file_id), True)

        # Create one block before assigning and one block after

        driver.register_block(vault_id, 'block_a', 1024)

        driver.assign_block(vault_id, file_id, 'block_a', 0)
        driver.assign_block(vault_id, file_id, 'block_b', 1024)

        driver.register_block(vault_id, 'block_b', 1024)

        self.assertEqual(driver.is_finalized(vault_id, file_id), False)

        driver.finalize_file(vault_id, file_id, 2048)

        self.assertEqual(driver.is_finalized(vault_id, file_id),
                         True)

    def test_file_assignment_gap_at_front(self):
        driver = self.create_driver()

        hdr_data = {
            'x-project-id': self.create_project_id(),
            'x-auth-token': ''
        }
        self.init_context(hdr_data)

        vault_id = self.create_vault_id()
        file_id = self.create_file_id()
        min_block_size = 101
        max_block_size = 10000

        num_blocks = 10

        driver.create_file(vault_id, file_id)

        block_ids = ['block_{0}'.format(id) for id in range(0, num_blocks)]
        block_sizes = [random.randrange(min_block_size, max_block_size)
                       for _ in range(0, num_blocks)]

        offsets = [sum(block_sizes[:x]) for x in range(0, num_blocks)]

        blocklist = list(zip(block_ids, block_sizes, offsets))
        file_size = sum(block_sizes)

        # register all of the blocks
        for block_id, block_size, offset in blocklist:
            driver.register_block(vault_id, block_id, block_size)

        # Remove the first set of blocks, creating a gap at the beginning of
        # the file
        _, required_offset, _ = blocklist[0]
        del blocklist[0]

        # now for fun randomize the order that we
        # will assign the blocks in
        # random.shuffle(blocklist)

        for block_id, block_size, offset in blocklist:
            driver.assign_block(vault_id, file_id, block_id, offset)

        with self.assertRaises(GapError) as ctx:
            driver.finalize_file(vault_id, file_id, file_size)

        self.assertEqual(ctx.exception.vault_id, vault_id)
        self.assertEqual(ctx.exception.startpos, 0)
        self.assertEqual(ctx.exception.endpos, required_offset)

    def test_file_assignment_overlap_error_in_middle(self):
        driver = self.create_driver()

        hdr_data = {
            'x-project-id': self.create_project_id(),
            'x-auth-token': ''
        }
        self.init_context(hdr_data)

        vault_id = self.create_vault_id()
        file_id = self.create_file_id()
        min_block_size = 101
        max_block_size = 10000

        num_blocks = 10

        driver.create_file(vault_id, file_id)

        block_ids = ['block_{0}'.format(id) for id in range(0, num_blocks)]
        block_sizes = [random.randrange(min_block_size, max_block_size)
                       for _ in range(0, num_blocks)]

        offsets = [sum(block_sizes[:x]) for x in range(0, num_blocks)]

        # Now take the second block's offset and subtract 10 from it's
        # offset. This should create an overlap with the second
        # and first block.
        expected_startpos = offsets[1] - 10
        expected_endpos = offsets[1]

        offsets[1] = offsets[1] - 10

        blocklist = list(zip(block_ids, block_sizes, offsets))
        file_size = sum(block_sizes)

        # register all of the blocks
        for block_id, block_size, offset in blocklist:
            driver.register_block(vault_id, block_id, block_size)

        random.shuffle(blocklist)

        for block_id, block_size, offset in blocklist:
            driver.assign_block(vault_id, file_id, block_id, offset)

        with self.assertRaises(OverlapError) as ctx:
            driver.finalize_file(vault_id, file_id, file_size)

        self.assertEqual(ctx.exception.vault_id, vault_id)
        self.assertEqual(ctx.exception.startpos, expected_startpos)
        self.assertEqual(ctx.exception.endpos, expected_endpos)

    def test_file_assignment_gap_at_back(self):
        driver = self.create_driver()

        hdr_data = {
            'x-project-id': self.create_project_id(),
            'x-auth-token': ''
        }
        self.init_context(hdr_data)

        vault_id = self.create_vault_id()
        file_id = self.create_file_id()
        min_block_size = 101
        max_block_size = 10000

        num_blocks = 10

        driver.create_file(vault_id, file_id)

        block_ids = ['block_{0}'.format(id) for id in range(0, num_blocks)]
        block_sizes = [random.randrange(min_block_size, max_block_size)
                       for _ in range(0, num_blocks)]

        offsets = [sum(block_sizes[:x]) for x in range(0, num_blocks)]

        blocklist = list(zip(block_ids, block_sizes, offsets))
        file_size = sum(block_sizes)

        # register all of the blocks
        for block_id, block_size, offset in blocklist:
            driver.register_block(vault_id, block_id, block_size)

        # Remove the last set of blocks, creating a gap at EOF
        missing_block = blocklist[-1]
        del blocklist[-1]

        random.shuffle(blocklist)

        for block_id, block_size, offset in blocklist:
            driver.assign_block(vault_id, file_id, block_id, offset)

        with self.assertRaises(GapError) as ctx:
            driver.finalize_file(vault_id, file_id, file_size)

    def test_file_assignment_overlap_at_back(self):
        driver = self.create_driver()

        hdr_data = {
            'x-project-id': self.create_project_id(),
            'x-auth-token': ''
        }
        self.init_context(hdr_data)

        vault_id = self.create_vault_id()
        file_id = self.create_file_id()
        min_block_size = 101
        max_block_size = 10000

        num_blocks = 10

        driver.create_file(vault_id, file_id)

        block_ids = ['block_{0}'.format(id) for id in range(0, num_blocks)]
        block_sizes = [random.randrange(min_block_size, max_block_size)
                       for _ in range(0, num_blocks)]

        offsets = [sum(block_sizes[:x]) for x in range(0, num_blocks)]

        blocklist = list(zip(block_ids, block_sizes, offsets))
        file_size = sum(block_sizes)

        # Now create the error: short the file size by 10 bytes
        expected_startpos = file_size - 10
        expected_endpos = file_size
        file_size -= 10

        # register all of the blocks
        for block_id, block_size, offset in blocklist:
            driver.register_block(vault_id, block_id, block_size)

        random.shuffle(blocklist)

        for block_id, block_size, offset in blocklist:
            driver.assign_block(vault_id, file_id, block_id, offset)

        with self.assertRaises(OverlapError) as ctx:
            driver.finalize_file(vault_id, file_id, file_size)

        self.assertEqual(ctx.exception.vault_id, vault_id)
        self.assertEqual(ctx.exception.startpos, expected_startpos)
        self.assertEqual(ctx.exception.endpos, expected_endpos)

    def test_file_block_generator(self):
        driver = self.create_driver()

        hdr_data = {
            'x-project-id': self.create_project_id(),
            'x-auth-token': ''
        }
        self.init_context(hdr_data)

        vault_id = self.create_vault_id()
        file_id = self.create_file_id()

        num_blocks = 40

        driver.create_file(vault_id, file_id)

        block_ids = ['block_{0}'.format(id) for id in range(0, num_blocks)]

        # Note: the mongo DB mocking driver is hard-coded to use
        # 40 1024-byte blocks.
        block_sizes = [1024 for _ in range(0, num_blocks)]

        offsets = [sum(block_sizes[:x]) for x in range(0, num_blocks)]

        blocklist = list(zip(block_ids, block_sizes, offsets))
        file_size = sum(block_sizes)

        # register all of the blocks
        for block_id, block_size, offset in blocklist:
            driver.register_block(vault_id, block_id, block_size)

        for block_id, block_size, offset in blocklist:
            driver.assign_block(vault_id, file_id, block_id, offset)

        driver.finalize_file(vault_id, file_id)

        output = list(driver.create_file_block_generator(vault_id, file_id))
        outblocks, outoffsets = zip(*output)

        self.assertEqual(list(outblocks), block_ids)
        self.assertEqual(list(outoffsets), offsets)

    def test_file_block_generator_marker_limit(self):
        driver = self.create_driver()

        hdr_data = {
            'x-project-id': self.create_project_id(),
            'x-auth-token': ''
        }
        self.init_context(hdr_data)

        vault_id = self.create_vault_id()
        file_id = self.create_file_id()

        num_blocks = 40

        driver.create_file(vault_id, file_id)

        block_ids = ['block_{0}'.format(id) for id in range(0, num_blocks)]

        # Note: the mongo DB mocking driver is hard-coded to use
        # 40 1024-byte blocks.
        block_sizes = [1024 for _ in range(0, num_blocks)]

        offsets = [sum(block_sizes[:x]) for x in range(0, num_blocks)]

        blocklist = list(zip(block_ids, block_sizes, offsets))
        file_size = sum(block_sizes)

        # register all of the blocks
        for block_id, block_size, offset in blocklist:
            driver.register_block(vault_id, block_id, block_size)

        for block_id, block_size, offset in blocklist:
            driver.assign_block(vault_id, file_id, block_id, offset)

        driver.finalize_file(vault_id, file_id)

        limit = 3
        offset = 0

        outblocks = []
        outoffsets = []

        iterations, remainder = divmod(num_blocks, limit)

        for _ in range(0, iterations):
            page = list(driver.create_file_block_generator(
                vault_id, file_id, offset=offset, limit=limit))

            self.assertEqual(len(page), limit)

            tempblocks, tempoffsets = zip(*page)
            outblocks.extend(tempblocks)
            outoffsets.extend(tempoffsets)

            offset = outoffsets[-1] + 1 if len(outoffsets) > 0 else None

        if remainder > 0:

            page = list(driver.create_file_block_generator(
                vault_id, file_id, offset=offset, limit=limit))

            self.assertEqual(len(page), remainder)

            tempblocks, tempoffsets = zip(*page)

            outblocks.extend(tempblocks)
            outoffsets.extend(tempoffsets)

        self.assertEqual(list(outblocks), block_ids)
        self.assertEqual(list(outoffsets), offsets)

        # Now try to do it again, this time with a ridiculous offset value
        out = list(driver.create_file_block_generator(vault_id, file_id,
                                                      offset=999999, limit=3))

        self.assertEqual(out, [])

    def test_block_generator(self):

        driver = self.create_driver()

        hdr_data = {
            'x-project-id': self.create_project_id(),
            'x-auth-token': ''
        }
        self.init_context(hdr_data)

        vault_id = self.create_vault_id()
        min_block_size = 101
        max_block_size = 10000

        num_blocks = 20

        block_ids = ['block_{0}'.format(id) for id in range(0, num_blocks)]
        block_sizes = [random.randrange(min_block_size, max_block_size)
                       for _ in range(0, num_blocks)]

        block_data = list(zip(block_ids, block_sizes))

        for block_id, block_size in block_data:
            driver.register_block(vault_id, block_id, block_size)

        outblocks = list(driver.create_block_generator(vault_id))

        self.assertEqual(sorted(block_ids), outblocks)

    def test_block_generator_marker_limit(self):

        driver = self.create_driver()

        hdr_data = {
            'x-project-id': self.create_project_id(),
            'x-auth-token': ''
        }
        self.init_context(hdr_data)

        vault_id = self.create_vault_id()
        min_block_size = 101
        max_block_size = 10000
        num_blocks = 20
        page_size = 3
        page_count, remainder = divmod(num_blocks, page_size)

        block_ids = ['block_{0:02d}'.format(id) for id in range(0, num_blocks)]
        block_sizes = [random.randrange(min_block_size, max_block_size)
                       for _ in range(0, num_blocks)]

        block_data = list(zip(block_ids, block_sizes))

        for block_id, block_size in block_data:

            driver.register_block(vault_id, block_id, block_size)

        marker = None
        limit = page_size + 1

        outblocks = []

        for x in range(0, page_count):
            page = list(driver.create_block_generator(vault_id, marker=marker,
                                                      limit=limit))

            self.assertEqual(len(page), limit)
            outblocks.extend(page[:-1])
            marker = page[-1]

        if remainder > 0:
            page = list(driver.create_block_generator(vault_id, marker=marker,
                                                      limit=limit))

            self.assertEqual(len(page), remainder)
            outblocks.extend(page)

        self.assertEqual(sorted(block_ids), outblocks)

    def test_file_generator(self):

        # Adds a bunch of files and checks the generator
        driver = self.create_driver()

        hdr_data = {
            'x-project-id': self.create_project_id(),
            'x-auth-token': self.create_auth_token()
        }
        self.init_context(hdr_data)

        vault_id = self.create_vault_id()
        num_files = 10

        # Create a list of 100 files
        file_ids = [self.create_file_id() for _ in range(0, num_files)]

        for file_id in file_ids:
            assert not driver.has_file(vault_id, file_id)

            out_id = driver.create_file(vault_id, file_id)

            self.assertEqual(out_id, file_id)
            assert driver.has_file(vault_id, file_id)

        # None of the files have been finalized so we should
        # get none back
        gen = driver.create_file_generator(vault_id)

        output = list(gen)

        self.assertEqual(output, [])

        # Now finalize the files and try to get them

        for file_id in file_ids:
            driver.finalize_file(vault_id, file_id)

        gen = driver.create_file_generator(vault_id)

        # Convert to a list of file ids.
        output = list(gen)

        # Why sorted? Deuce returns file ids sorted, but the list
        # of file ids that we generated were not sorted when we
        # created them above.
        self.assertEqual(output, sorted(file_ids))

        # Now try with markers
        gen = driver.create_file_generator(vault_id,
                                           marker=sorted(file_ids)[2], limit=3)

        output = list(gen)
        self.assertEqual(len(output), 3)  # Limited query to 3

        # We should be able to compare the list of
        # file ids here with the ones that come from
        # the other list
        target_list = sorted(file_ids)[2:5]

        self.assertEqual(target_list, output)

    def test_vault_crud_and_generator(self):
        driver = self.create_driver()
        vaultids = list()
        for n in range(5):
            vault_id = self.create_vault_id()
            driver.create_vault(vault_id)
            vaultids.append(vault_id)

        driver.create_vaults_generator(marker=None, limit=99)
        driver.create_vaults_generator(marker=vaultids[0], limit=99)

        for vault_id in vaultids:
            driver.delete_vault(vault_id)
