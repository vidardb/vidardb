## VidarDB: A Versatile Storage Engine that Adapts to Your Workloads

[![Build Status](https://travis-ci.org/vidardb/vidardb.svg?branch=master)](https://travis-ci.org/vidardb/vidardb)

VidarDB is developed and maintained by VidarDB Team.
It is built on RocksDB and earlier work on LevelDB by Sanjay Ghemawat (sanjay@google.com)
and Jeff Dean (jeff@google.com)

This code is a library that forms the core building block for a fast
key value server.

The public interface is in `include/`.  Callers should not include or
rely on the details of any other header files in this package.  Those
internal APIs may be changed without warning.
