
import uuid


class Session(object):

    def __init__(self, conn):
        self.conn = conn

    def execute(self, query, queryargs):
        # Cassandra driver supports UUID type natively but sqlite3
        # does not, so convert every UUID to a string.
        queryargs = tuple([str(s) if isinstance(s, uuid.UUID)
                           else s for s in queryargs])

        query = query.replace('false', '0')
        query = query.replace('true', '1')
        query = query.replace('%s', '?')

        res = self.conn.execute(query, queryargs)
        res = list(res)

        return res
