Deuce [![Build Status](https://api.travis-ci.org/rackerlabs/deuce.png)](https://travis-ci.org/rackerlabs/deuce)
=====

Block-Level De-duplication as a Service. 

Note: Deuce is a work in progress and is not currently recommended for production use.

What is Deuce?
--------------
In today's web-enabled world we generate a *lot* of data. This is especially true in the brave new cloud-based IaaS offerings where servers can easily live and die within a matter of minutes. The nature of this data means that a lot of the data is redundant, especially subsets of files. Deuce aims to allow a user to easily store his or her data from disparate sources while de-duplicating the data and storing the resulant blocks and metadata in trusted, durable backend stores (i.e. Open Stack swift, Cassandra, etc). Once a file has been successfully stored in Deuce, it can be retrieved via a simple GET operation. 

Focus of this project is intentionally very narrow, following the *NIX notion of small tools that can be pieced together with other tools to do amazing things.

API
---

Fine details of the API are still being worked out and are subject to change. API docs are available [here](http://docs.deuce.apiary.io).
API testing collection for Postman Google Chrome is in docs/deuce_postman.json

Features
--------
 * Python 3.3 and 3.4 are currently supported
 * Client-side de-duplication
 * Server-side reconstruction and retrieval of de-duplicated data
 * Pluggable driver support for metadata and block backend stores
 * Cassandra, MongoDB and sqlite drivers for metadata storage
 * Disk storage and Open Stack Swift supported for block storage
 * Designed from the ground up to work well in Open Stack environments

What Deuce is not?
------------------
 * A backup program. It is ideal for being used by a backup system for implementing data, but it in itself is not a backup program. 
 * Block storage. In spite of using some common words, Deuce does not aim to compete with block storage solutions such as Open Stack cinder.
 * Object storage (such as Open Stack Swift)


Installation
------------
Trying out Deuce is simple. The default configuration is setup to use sqlite and disk storage drivers. This means that you can be up and running quickly for development and evaluation purposes. 


Clone this repo: 

```
  git clone https://github.com/rackerlabs/deuce.git
```

Install the code

```
  cd deuce
  virtualenv env
  . env/bin/activate
  python setup.py develop
```

Start it up

```
  pecan serve config.py
```
