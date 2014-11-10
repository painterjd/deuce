import six


class FileCat(object):

    """FileCat: Allows multiple files to be handled
    as a single file-like-object for read-only
    operations. Seek is not supported"""

    def __init__(self, fileobjs):
        """Constructs a new FileCat object.
        :param fileobjs: Any iterable capable of
            returning file-like objects. The objects
            must be ready-to-read
        """
        self._objs = fileobjs
        self._current_file = None
        if self._objs:
            try:
                self._current_file = next(self._objs)
            except StopIteration:
                self._current_file = None

    def read(self, count=None):

        res = six.binary_type()

        if not self._current_file:
            return res

        while count is None or len(res) < count:

            bytes_to_read = count - len(res) if count else None

            buff = self._current_file.read(bytes_to_read)

            if len(buff) == 0:  # End of file

                # Close this file. We're going
                # to move on to the next one
                self._current_file.close()

                try:
                    self._current_file = next(self._objs)
                except StopIteration:
                    # We are done, just break and
                    # return whatever we've already
                    # read (if anything)
                    self._current_file = None
                    break
            else:
                res += buff

        return res
