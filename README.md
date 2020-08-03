<img style="width:100%;" src="/github-banner.png">

## VidarDB engine is a versatile storage engine. It is a lineage of LevelDB and RocksDB, but with support of various workloads.

[![Build Status](https://travis-ci.com/vidardb/vidardb-engine.svg?branch=master)](https://travis-ci.com/github/vidardb/vidardb-engine)

This code is a library that forms the core building block for VidarDB database system. For ease of use, we provide a [Docker image](docker_image/README.md) that has a PostgreSQL interface and SQL is supported. You can read more documentation [here](https://www.vidardb.com/docs/). VidarDB is actively developed and maintained by VidarDB team. Feel free to report bugs or issues via Github.

If you are interested in contributing to VidarDB, see [CONTRIBUTING.md](./CONTRIBUTING.md).

### Building

- For static library:

    ```shell
    sudo DEBUG_LEVEL=0 make static_lib
    ```

- For shared library:

    ```shell
    sudo DEBUG_LEVEL=0 make shared_lib
    ```

### Installing

```
sudo make install
```
