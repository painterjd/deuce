from unittest import TestCase
from deuce.util import FileCat, set_qs, set_qs_on_url
from deuce.tests.util import MockFile
import os
from random import randrange
from hashlib import md5

try:  # pragma: no cover
    import six.moves.urllib.parse as parse
except ImportError:  # pragma: no cover
    import urllib.parse as parse

# TODO: We probably want to move this to a
# test helpers library


class TestFileCat(TestCase):

    def test_full_read(self):
        num_files = 9
        min_file_size = 1
        max_file_size = 5

        file_sizes = [randrange(min_file_size, max_file_size)
                      for i in range(0, num_files)]

        files = [MockFile(size) for size in file_sizes]

        # Calculate an md5 of all of our files.
        z = md5()
        for f in files:
            z.update(f._content)

        expected_size = sum(file_sizes)
        expected_md5 = z.hexdigest()

        # Pass None to FileCat
        fc = FileCat(None)

        # Pass empty list to FileCat
        fc = FileCat((f for f in range(1, 0)))

        # FileCat only takes generators
        fc = FileCat((f for f in files))

        data = fc.read()  # read it all

        z = md5()
        z.update(data)
        computed_md5 = z.hexdigest()

        assert len(data) == sum(file_sizes)
        assert computed_md5 == expected_md5

    def test_small_read(self):
        num_files = 7
        min_file_size = 0
        max_file_size = 10000

        file_sizes = [randrange(min_file_size, max_file_size)
                      for i in range(0, num_files)]

        files = [MockFile(size) for size in file_sizes]

        # Calculate an md5 of all of our files.
        z = md5()
        for f in files:
            z.update(f._content)

        expected_size = sum(file_sizes)
        expected_md5 = z.hexdigest()

        # FileCat only takes generators
        fc = FileCat((f for f in files))

        z = md5()
        bytes_read = 0

        while True:
            buff = fc.read(99)
            assert len(buff) <= 99

            if len(buff) == 0:
                break  # DONE

            bytes_read += len(buff)
            z.update(buff)

        computed_md5 = z.hexdigest()

        assert bytes_read == sum(file_sizes)
        assert computed_md5 == expected_md5

    def test_set_qs_on_url(self):
        url = 'http://whatever:8080/hello/world'

        # Empty case
        query_string = set_qs(url, args={'param1': 'value1'})
        self.assertEqual('param1=value1', query_string)

    def test_set_qs(self):
        url = 'http://whatever:8080/hello/world?param1=value1&param2=value2'

        # Empty case
        testurl = set_qs_on_url(url)
        self.assertEqual(testurl, 'http://whatever:8080/hello/world')

        positive_cases = [
            {'whatever': '3'},
            {'hello': 'whatever'},
            {'yes': u'whatever'}
        ]

        for args in positive_cases:
            output = set_qs(url, args)
            parts = parse.urlparse(output)

            qs = parts.query
            output = parse.parse_qs(qs)
