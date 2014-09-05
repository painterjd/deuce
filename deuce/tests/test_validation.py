from unittest import TestCase
from deuce.util import FileCat

from deuce.controllers.validation import ValidationFailed
from deuce.controllers import validation as v
import os

# TODO: We probably want to move this to a
# test helpers library

VALIDATED_STR = 'validated'


class MockRequest(object):
    pass


class MockResponse(object):
    pass

request = MockRequest()
response = MockResponse()


@v.validation_function
def is_upper(z):
    """Simple validation function for testing purposes
    that ensures that input is all caps
    """
    if z.upper() != z:
        raise v.ValidationFailed('{0} no uppercase'.format(z))

error_count = 0


def abort(code):
    global error_count
    error_count = error_count + 1

other_vals = dict()
get_other_val = other_vals.get


class DummyEndpoint(object):

    # This should throw a ValidationProgrammingError
    # when called because the user did not actually
    # call validate_upper.

    # Note: the lambda in this function can never actually be
    # called, so we use no cover here
    @v.validate(value=v.Rule(is_upper, lambda: abort(404)))  # pragma: no cover
    def get_value_programming_error(self, value):
        # This function body should never be
        # callable since the validation error
        # should not allow it to be called
        assert False  # pragma: no cover

    @v.validate(
        value1=v.Rule(is_upper(), lambda: abort(404)),
        value2=v.Rule(is_upper(), lambda: abort(404)),
        value3=v.Rule(is_upper(), lambda: abort(404))
    )  # pragma: no cover
    def get_value_happy_path(self, value1, value2, value3):
        return value1 + value2 + value3

    @v.validate(
        value1=v.Rule(is_upper(), lambda: abort(404)),
        value2=v.Rule(is_upper(empty_ok=True), lambda: abort(404),
                      get_other_val),
    )  # pragma: no cover
    def get_value_with_getter(self, value1):
        global other_vals
        return value1 + other_vals.get('value2')


class TestValidationDecorator(TestCase):

    def setUp(self):
        self.ep = DummyEndpoint()

    def test_programming_error(self):
        with self.assertRaises(v.ValidationProgrammingError):
            self.ep.get_value_programming_error('AT_ME')

    def test_happy_path_and_validation_failure(self):
        global error_count
        # Should not throw
        res = self.ep.get_value_happy_path('WHATEVER', 'HELLO', 'YES')
        self.assertEqual('WHATEVERHELLOYES', res)

        # Validation should have failed, and
        # we should have seen a tick in the error count
        oldcount = error_count
        res = self.ep.get_value_happy_path('WHAtEVER', 'HELLO', 'YES')
        self.assertEqual(oldcount + 1, error_count)

        # Check passing a None value. This decorator does
        # not permit none values.
        oldcount = error_count
        res = self.ep.get_value_happy_path(None, 'HELLO', 'YES')
        self.assertEqual(oldcount + 1, error_count)

    def test_getter(self):
        global other_vals

        other_vals['value2'] = 'HELLO'

        # Now have our validation actually try to
        # get those values

        # This should succeed
        res = self.ep.get_value_with_getter('TEST')
        self.assertEqual('TESTHELLO', res)

        # check empty_ok
        other_vals['value2'] = ''
        res = self.ep.get_value_with_getter('TEST')
        self.assertEqual('TEST', res)


class TestValidationFuncs(TestCase):

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

    def test_rules(self):
        # Tests each rule to ensure that empty and other
        # cases work

        rules = {v.VaultGetRule, v.VaultPutRule, v.BlockGetRule,
                 v.FileGetRule, v.FilePostRuleNoneOk,
                 v.BlockPutRuleNoneOk, v.FileMarkerRule, v.VaultMarkerRule,
                 v.OffsetMarkerRule, v.BlockMarkerRule, v.LimitRule}

        for rule in rules:
            with self.assertRaises(ValidationFailed):
                v.val_limit()('')

            with self.assertRaises(ValidationFailed):
                v.val_limit()(None)
