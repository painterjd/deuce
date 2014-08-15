
import sqlite3
import uuid
import collections

MOCK_CASSANDRA_SCHEMA = ["""
CREATE TABLE blocks (
  projectid TEXT,
  vaultid TEXT,
  blockid TEXT,
  blocksize INT,
  PRIMARY KEY(projectid, vaultid, blockid)
);
""", """
CREATE TABLE files (
  projectid TEXT,
  vaultid TEXT,
  fileid TEXT,
  size INT,
  finalized BOOLEAN,
  PRIMARY KEY(projectid, vaultid, fileid)
);
""", """
CREATE INDEX finalized_index
    on files (finalized);
""", """
CREATE TABLE fileblocks (
  projectid TEXT,
  vaultid TEXT,
  fileid TEXT,
  blockid TEXT,
  blocksize INT,
  offset INT,
  PRIMARY KEY(projectid, vaultid, fileid, offset)
);
"""]

from deuce.tests.mock_cassandra import Session


class Cluster(object):

    def __init__(self, hosts):
        # Create the mock driver in memory only
        self._sqliteconn = sqlite3.connect(':memory:')

        for stmt in MOCK_CASSANDRA_SCHEMA:
            self._sqliteconn.execute(stmt)

    def connect(self, keyspace):
        return Session(self._sqliteconn)
