from unittest import TestCase

from falcon import request
from stoplight.exceptions import ValidationFailed

from deuce.transport import validation as v


class MockRequest(object):
    pass


class TestValidationFuncs(TestCase):

    def test_request(self):
        mock_env = {
            'wsgi.errors': 'mock',
            'wsgi.input': 'mock',
            'REQUEST_METHOD': 'PUT',
            'PATH_INFO': '/',
            'SERVER_NAME': 'mock',
            'SERVER_PORT': '8888',

        }
        positive_case = [request.Request(mock_env)]
        for case in positive_case:
            v.is_request(case)
        negative_case = [MockRequest()]
        for case in negative_case:
            with self.assertRaises(ValidationFailed):
                v.is_request(none_ok=True)(case)

    def test_vault_id(self):

        positive_cases = [
            'a',
            '0',
            '__vault_id____',
            '-_-_-_-_-_-_-_-',
            'snake_case_is_ok',
            'So-are-hyphonated-names',
            'a' * v.VAULT_ID_MAX_LEN
        ]

        for name in positive_cases:
            v.val_vault_id(name)

        negative_cases = [
            '',  # empty case should raise
            '.', '!', '@', '#', '$', '%',
            '^', '&', '*', '[', ']', '/',
            '@#$@#$@#^@%$@#@#@#$@!!!@$@$@',
            '\\', 'a' * (v.VAULT_ID_MAX_LEN + 1)
        ]

        for name in negative_cases:
            with self.assertRaises(ValidationFailed):
                v.val_vault_id()(name)

    def test_block_id(self):

        positive_cases = [
            'da39a3ee5e6b4b0d3255bfef95601890afd80709',
            'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            'ffffffffffffffffffffffffffffffffffffffff',
            'a' * 40,
        ]

        for blockid in positive_cases:
            v.val_block_id(blockid)

        negative_cases = [
            '',
            '.',
            'a', '0', 'f', 'F', 'z', '#', '$', '?',
            'a39a3ee5e6b4b0d3255bfef95601890afd80709',  # one char short
            'da39a3ee5e6b4b0d3255bfef95601890afd80709a',  # one char long
            'DA39A3EE5E6B4B0D3255BFEF95601890AFD80709',
            'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF',
            'AaaAaaAaaaaAaAaaaAaaaaaaaAAAAaaaaAaaaaaa' * 2,
            'AaaAaaAaaaaAaAaaaAaaaaaaaAAAAaaaaAaaaaaa' * 3,
            'AaaAaaAaaaaAaAaaaAaaaaaaaAAAAaaaaAaaaaaa' * 4
        ]

        for blockid in negative_cases:
            with self.assertRaises(ValidationFailed):
                v.val_block_id()(blockid)

    def test_file_id(self):

        import uuid

        # Let's try try to append some UUIds and check for faileus
        positive_cases = [str(uuid.uuid4()) for _ in range(0, 1000)]

        for fileid in positive_cases:
            v.val_file_id(fileid)

        negative_cases = [
            '',
            'e7bf692b-ec7b-40ad-b0d1-45ce6798fb6z',  # note trailing z
            str(uuid.uuid4()).upper()  # Force case sensitivity
        ]

        for fileid in negative_cases:
            with self.assertRaises(ValidationFailed):
                v.val_file_id()(fileid)

    def test_offset(self):
        positive_cases = [
            '0', '1', '2', '3', '55', '100',
            '101010', '99999999999999999999999999999'
        ]

        for offset in positive_cases:
            v.val_offset()(offset)

        negative_cases = [
            '-1', '-23', 'O', 'zero', 'one', '-999', '1.0', '1.3',
            '0.0000000000001'
        ]

        for offset in negative_cases:
            with self.assertRaises(ValidationFailed):
                v.val_offset()(offset)

    def test_limit(self):
        positive_cases = [
            '0', '100', '100000000', '100'
        ]

        for limit in positive_cases:
            v.val_limit()(limit)

        negative_cases = [
            '-1', 'blah', None
        ]

        for limit in negative_cases:
            with self.assertRaises(ValidationFailed):
                v.val_limit()(limit)

        v.val_limit(empty_ok=True)('')
        v.val_limit(none_ok=True)(None)

        with self.assertRaises(ValidationFailed):
            v.val_limit()('')

        with self.assertRaises(ValidationFailed):
            v.val_limit()(None)
