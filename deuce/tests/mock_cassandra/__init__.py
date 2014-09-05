

import deuce.drivers.cassandra.cassandrametadatadriver \
    as actual_driver

import uuid


class Session(object):

    def __init__(self, conn):
        self.conn = conn

    def execute(self, query, queryargs):
        # Health check.
        if 'system.local' in query:
            return 'true'

        original_query = query

        query = query.replace('false', '0')
        query = query.replace('true', '1')

        if isinstance(queryargs, tuple):

            # convert UUID to string
            queryargs = tuple([str(s) if isinstance(s, uuid.UUID)
                           else s for s in queryargs])

            # sqlite prefers ? over %s for positional args
            query = query.replace('%s', '?')

        elif isinstance(queryargs, dict):

            # If the user passed dictionary arguments, assume that they
            # used that cassandra %(fieldname)s and convert to sqlite's
            # :fieldname

            for k, v in queryargs.items():
                cass_style_arg = "%({0})s".format(k)
                sqlite_style_arg = ":{0}".format(k)
                query = query.replace(cass_style_arg, sqlite_style_arg)

                # Convert UUID parameters to strings
                if isinstance(v, uuid.UUID):
                    queryargs[k] = str(v)

        if original_query == actual_driver.CQL_INC_BLOCK_REF_COUNT:

            # Special-case this query, since sqlite doesn't
            # support upserts

            insert_query = """
                INSERT or IGNORE into blockreferences
                (projectid, vaultid, blockid, refcount)
                VALUES
                (:projectid, :vaultid, :blockid, :refcount)
            """

            insert_args = queryargs.copy()
            insert_args.update({'refcount': 0})
            del insert_args["delta"]

            self.conn.execute(insert_query, insert_args)

        res = self.conn.execute(query, queryargs)
        res = list(res)

        return res
