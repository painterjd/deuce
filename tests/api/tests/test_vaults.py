from tests.api import base
from tests.api.utils.schema import deuce_schema

import ddt
import jsonschema
import urlparse
import time


class TestNoVaultsCreated(base.TestBase):
    _multiprocess_can_split = True

    def setUp(self):
        super(TestNoVaultsCreated, self).setUp()

    def test_head_missing_vault(self):
        """Head of a vault that has not been created"""

        resp = self.client.vault_head(self.id_generator(50))
        self.assert_404_response(resp, skip_contentlength=True)

    def test_get_missing_vault(self):
        """Get a vault that has not been created"""

        resp = self.client.get_vault(self.id_generator(50))
        self.assert_404_response(resp)

    def test_delete_missing_vault(self):
        """Delete a missing Vault"""

        resp = self.client.delete_vault(self.id_generator(55))
        self.assert_404_response(resp)

    def tearDown(self):
        super(TestNoVaultsCreated, self).tearDown()


@ddt.ddt
class TestCreateVaults(base.TestBase):
    _multiprocess_can_split = True

    def setUp(self):
        super(TestCreateVaults, self).setUp()

    @ddt.data(1, 10, 100, 128)
    def test_create_vaults(self, size):
        """Create a Vault"""

        self.vaultname = self.id_generator(size)
        resp = self.client.create_vault(self.vaultname)
        self.assert_201_response(resp)

    def tearDown(self):
        super(TestCreateVaults, self).tearDown()
        if hasattr(self, 'vaultname'):
            self.client.delete_vault(self.vaultname)


class TestEmptyVault(base.TestBase):
    _multiprocess_can_split_ = True

    def setUp(self):
        super(TestEmptyVault, self).setUp()
        self.create_empty_vault()

    def test_get_vault(self):
        """Get an individual vault. Get the statistics for a vault"""

        resp = self.client.get_vault(self.vaultname)
        self.assert_200_response(resp)

        resp_body = resp.json()
        jsonschema.validate(resp_body, deuce_schema.vault_statistics)

        storage = resp_body['storage']
        self.assertEqual(storage['block-count'], 0)
        self.assertEqual(storage['total-size'], 0)

        if 'last-modification-time' in storage['internal']:
            slmt = storage['internal']['last-modification-time']
            try:
                time.ctime(float(slmt))
            except Exception:
                self.fail('internal/last-modification-time {0} is not time'
                        ''.format(slmt))
        else:
            self.assertEqual(storage['internal'], {})

        meta = resp_body['metadata']

        meta_files = meta['files']
        self.assertEqual(meta_files['count'], 0)

        self.assertEqual(meta['internal'], {})

        meta_blocks = meta['blocks']
        self.assertEqual(meta_blocks['count'], 0)

    def test_delete_vault(self):
        """Delete a Vault"""

        resp = self.client.delete_vault(self.vaultname)
        self.assert_204_response(resp)

    def test_vault_head(self):
        """Head of an individual vault"""

        resp = self.client.vault_head(self.vaultname)
        self.assert_204_response(resp)

    def tearDown(self):
        super(TestEmptyVault, self).tearDown()
        self.client.delete_vault(self.vaultname)


class TestVaultWithBlocksFiles(base.TestBase):
    _multiprocess_can_split = True

    def setUp(self):
        super(TestVaultWithBlocksFiles, self).setUp()
        self.create_empty_vault()
        self.upload_multiple_blocks(20)
        [self.create_new_file() for _ in range(3)]
        # Assign specific blocks to the files created
        # Assign 3 unique blocks to file 1
        self.assign_blocks_to_file(blocks=[0, 1, 2],
                                   file_url=self.files[0].Url)

        # Assign 5 blocks to file 2, sharing 2 blocks with file 1
        self.filesize = 0
        self.assign_blocks_to_file(blocks=[4, 5, 0, 6, 2],
                                   file_url=self.files[1].Url)

        # Assign 8 unique blocks to file 3 and finalize it
        self.filesize = 0
        self.assign_blocks_to_file(blocks=range(10, 18),
                                   file_url=self.files[2].Url)
        self.finalize_file(file_url=self.files[2].Url)
        # 14 blocks have been assigned to 3 files

    def test_get_populated_vault(self):
        """Get the statistics of a populated vault"""

        resp = self.client.get_vault(self.vaultname)
        self.assert_200_response(resp)

        resp_body = resp.json()
        jsonschema.validate(resp_body, deuce_schema.vault_statistics)

        storage = resp_body['storage']
        self.assertEqual(storage['block-count'], 20)
        self.assertEqual(storage['total-size'], 30720 * 20)

        if 'last-modification-time' in storage['internal']:
            slmt = storage['internal']['last-modification-time']
            try:
                time.ctime(float(slmt))
            except Exception:
                self.fail('internal/last-modification-time {0} is not time'
                        ''.format(slmt))
        else:
            self.assertEqual(storage['internal'], {})

        meta = resp_body['metadata']
        meta_files = meta['files']
        self.assertEqual(meta_files['count'], 3)

        self.assertEqual(meta['internal'], {})

        meta_blocks = meta['blocks']
        self.assertEqual(meta_blocks['count'], 20)

    def test_populated_vault_head(self):
        """Head of an individual, populated vault"""

        resp = self.client.vault_head(self.vaultname)
        self.assert_204_response(resp)

    def tearDown(self):
        super(TestVaultWithBlocksFiles, self).tearDown()
        [self.client.delete_file(vaultname=self.vaultname,
            fileid=fileid.Id) for fileid in self.files]
        [self.client.delete_block(self.vaultname, block.Id) for block in
            self.blocks]
        self.client.delete_vault(self.vaultname)


class TestPopulatedVault(base.TestBase):
    _multiprocess_can_split = True

    def setUp(self):
        super(TestPopulatedVault, self).setUp()
        self.create_empty_vault()
        self.upload_block()

    def test_delete_populated_vault(self):
        """Delete a Vault that has some data. 1 block"""

        resp = self.client.delete_vault(self.vaultname)
        self.assert_409_response(resp)

        resp_body = resp.json()
        jsonschema.validate(resp_body, deuce_schema.error)

        self.assertEqual(resp_body['title'], 'Conflict')
        self.assertEqual(resp_body['description'], 'Vault cannot be deleted')

    def tearDown(self):
        super(TestPopulatedVault, self).tearDown()
        [self.client.delete_block(self.vaultname, block.Id) for block in
            self.blocks]
        self.client.delete_vault(self.vaultname)


@ddt.ddt
class TestListVaults(base.TestBase):
    _multiprocess_can_split_ = True

    def setUp(self):
        super(TestListVaults, self).setUp()
        self.vaults = []
        [self.create_empty_vault() for _ in range(20)]
        self.vaultids = sorted(self.vaults[:])

    def check_vaultids_in_resp(self, vaultids, response):
        resp_body = response.json()
        jsonschema.validate(resp_body, deuce_schema.vault_list)

        for vaultid in resp_body.keys():
            # check that the vaultid was among the created ones
            if self.soft_vault_list_validation:
                if vaultid not in vaultids:
                    continue
            self.assertIn(vaultid, vaultids)
            vaultids.remove(vaultid)
            # check the url in the response
            vault_url = resp_body[vaultid]['url']
            self.assertUrl(vault_url, vaultspath=True)
            vault_url = urlparse.urlparse(vault_url)
            vaultid_from_url = vault_url.path.split('/')[-1]
            self.assertEqual(vaultid_from_url, vaultid)

    @ddt.data(2, 4, 5, 10)
    def test_list_vaults_limit(self, value):
        """List multiple vaults, setting the limit to value"""

        self.assertVaultsPerPage(value)

    @ddt.data(2, 4, 5, 10)
    def test_list_vaults_limit_marker(self, value):
        """List multiple vaults, setting the limit to value and using a
        marker"""

        markerid = self.vaultids[value]
        self.assertVaultsPerPage(value, marker=markerid, pages=1)

    def assertVaultsPerPage(self, value, marker=None, pages=0):
        """
        Helper function to check the vaults returned per request
        Also verifies that the marker, if provided, is used
        """

        url = None
        finished = False
        while True:
            if not url:
                resp = self.client.list_of_vaults(marker=marker, limit=value)
            else:
                resp = self.client.list_of_vaults(alternate_url=url)

            self.assert_200_response(resp)

            resp_body = resp.json()
            if len(resp_body.keys()) == value:
                if 'x-next-batch' in resp.headers:
                    url = resp.headers['x-next-batch']
                    self.assertUrl(url, vaults=True, nextlist=True)
                else:
                    finished = True
            else:
                self.assertNotIn('x-next-batch', resp.headers)
                finished = True

            self.check_vaultids_in_resp(self.vaultids, resp)
            if finished:
                break
        self.assertEqual(len(self.vaultids), value * pages,
                         'Discrepancy between the list of vaults returned '
                         'and the vaults uploaded {0}'.format(self.vaultids))

    def test_list_vault_invalid_marker(self):
        """Request a Vault list with an invalid marker"""

        bad_marker = '#$@#@%$#fjsd0-'
        resp = self.client.list_of_vaults(marker=bad_marker)
        self.assert_404_response(resp)

    def test_list_vault_bad_marker(self):
        """Request a Vault List with a bad marker.
        The marker is correctly formatted, but does not exist"""

        while True:
            bad_marker = self.id_generator(50)
            if bad_marker not in self.vaults:
                break
        vaults = self.vaults[:]
        vaults.append(bad_marker)
        vaults.sort()
        i = vaults.index(bad_marker)

        resp = self.client.list_of_vaults(marker=bad_marker)
        resp_list = []
        while True:
            self.assert_200_response(resp)
            resp_body = resp.json()
            jsonschema.validate(resp_body, deuce_schema.vault_list)
            resp_list += sorted(resp_body.keys())
            if 'x-next-batch' in resp.headers:
                resp = self.client.list_of_vaults(
                    alternate_url=resp.headers['x-next-batch'])
            else:
                break

        if self.soft_vault_list_validation:
            for vaultname in vaults[i + 1:]:
                self.assertIn(vaultname, resp_list)
        else:
            self.assertEqual(sorted(resp_body.keys()), vaults[i + 1:])

    def tearDown(self):
        super(TestListVaults, self).tearDown()
        [self.client.delete_vault(vault) for vault in self.vaults]
