
// TODO: Tweak this replication strategy
CREATE KEYSPACE IF NOT EXISTS deucekeyspace WITH
  replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

use deucekeyspace;

CREATE TABLE vaults(
  projectid TEXT,
  vaultid TEXT,
  PRIMARY KEY(projectid, vaultid)
);

CREATE COLUMNFAMILY blocks (
  projectid TEXT,
  vaultid TEXT,
  blockid TEXT,
  storageid TEXT,
  blocksize INT,
  reftime BIGINT,
  isinvalid BOOLEAN,
  PRIMARY KEY((projectid, vaultid), blockid)
);

CREATE INDEX storageid_index
    on blocks (storageid);

CREATE INDEX invalid_block_index
    on blocks (isinvalid);

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

CREATE COLUMNFAMILY blockreferences (
  projectid TEXT,
  vaultid TEXT,
  blockid TEXT,
  refcount COUNTER,
  PRIMARY KEY(projectid, vaultid, blockid) 
);

CREATE TABLE fileblocks (
  projectid TEXT,
  vaultid TEXT, 
  fileid UUID,
  blockid TEXT,
  blocksize INT,
  offset BIGINT,
  PRIMARY KEY((projectid, vaultid, fileid), offset)
);
