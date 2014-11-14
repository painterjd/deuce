# Either use real Python MongoDB module...
#
# import pymongo
# from pymongo import MongoClient as mongoclient
#
# Or from a mocking package...

from mongomock.connection import Connection


class Mock_Connection(Connection):

    def __init__(self, *args, **kwargs):
        super(Mock_Connection, self).__init__(*args, **kwargs)

    def alive(self):
        """The original MongoConnection.alive method checks the
        status of the server.
        In our case as we mock the actual server, we should always return True.
        """
        return True


def MongoClient(url):
    return Mock_Connection()
