
// TODO: Tweak this replication strategy
CREATE KEYSPACE IF NOT EXISTS deucekeyspace WITH 
  replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

use deucekeyspace; 

CREATE COLUMNFAMILY blocks (
  projectid TEXT,
  vaultid TEXT,
  blockid TEXT,
  blocksize INT,
  PRIMARY KEY((projectid, vaultid), blockid) 
);

CREATE TABLE files (
  projectid TEXT,
  vaultid TEXT,
  fileid UUID,
  size BIGINT,
  finalized BOOLEAN,
  PRIMARY KEY((projectid, vaultid), fileid)
);

CREATE INDEX finalized_index
    on files (finalized);

CREATE TABLE fileblocks (
  projectid TEXT,
  vaultid TEXT, 
  fileid UUID,
  blockid TEXT,
  blocksize INT,
  offset BIGINT,
  PRIMARY KEY((projectid, vaultid, fileid), offset)
);
