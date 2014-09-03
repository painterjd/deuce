
# Either use real Python MongoDB module...
#
# import pymongo
# from pymongo import MongoClient as mongoclient
#
# Or from a mocking package...
import math
from mongomock.connection import Connection
from mongomock.collection import Collection


# Mongomock does not have aggregate impl.
# Patch a quick impl.
def patch_aggregate(self, query):

    max_blocks = 40

    limit = 0
    retval = dict()
    retval['result'] = []
    blocks = []
    blocksdict = dict()
    offset = 0

    for item in query:
        if '$match' in item and 'blocks.offset' in item['$match']:
            offset = item['$match']['blocks.offset']['$gte']
        if '$limit' in item:
            limit = item['$limit']

    if limit > max_blocks:
        limit = max_blocks

    block_index = int(math.ceil(offset / 1024.0))
    end_index = min(block_index + limit, max_blocks)

    for x in range(block_index, end_index):
        block = {}
        block['blockid'] = 'block_{0}'.format(x)
        block['offset'] = x * 1024
        blocks.append(block.copy())

    blocksdict['blocks'] = blocks
    retval['result'].append(blocksdict.copy())

    return retval

old_init = Collection.__init__


def new_init(self, db, name):
    old_init(self, db, name)

Collection.aggregate = patch_aggregate
Collection.__init__ = new_init


def MongoClient(url):
    # Either use real Python MongoDB module...
    #
    # return mongoclient(url)
    #
    # Or from a mocking package...
    return Connection()
