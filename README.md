<img style="width:100%;" src="/github-banner.png">

## VidarDB: the world's first unified database system, replacing multiple data tools with a single unified database.

[![Build Status](https://travis-ci.com/vidardb/vidardb.svg?branch=master)](https://travis-ci.com/github/vidardb/vidardb)

This code is a library that forms the core building block for VidarDB database system. For ease of use, we provide a [Docker image](docker_image/README.md) that has a PostgreSQL interface and SQL is supported. Please note, it is not product-ready. You can read more documentation [here](https://www.vidardb.com/docs/). VidarDB is actively developed and maintained by VidarDB team. Feel free to report bugs or issues via Github.

If you are interested in contributing to VidarDB, see [CONTRIBUTING.md](./CONTRIBUTING.md).

### Building

- For static library:

    ```shell
    sudo DEBUG_LEVEL=0 make static_lib install-static
    ```

- For shared library:

    ```shell
    sudo DEBUG_LEVEL=0 make shared_lib install-shared
    ```
