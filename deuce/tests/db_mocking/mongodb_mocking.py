# Either use real Python MongoDB module...
#
# import pymongo
# from pymongo import MongoClient as mongoclient
#
# Or from a mocking package...

from mongomock.connection import Connection


def MongoClient(url):
    return Connection()
