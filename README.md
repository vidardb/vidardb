<img style="width:100%;" src="/github-banner.png">

## VidarDB engine is a modern embedded key-value store with a versatile storage engine. It is a lineage of LevelDB and RocksDB, but with support of various workloads.

![Github Action Build Status](https://github.com/vidardb/vidardb-engine/workflows/CI/badge.svg)

There are 3 interfaces to use it: C++, Python, PostgreSQL extension.

For C++: build either by cmake or makefile (see Building & Installing steps below) provided, then try [examples](examples/simple_example.cc).

For Python: check this [repo](https://github.com/vidardb/PyVidarDB).

For PostgreSQL extension: check this [repo](https://github.com/vidardb/PostgresForeignDataWrapper). For ease of use, we provide a [Docker image](docker_image/README.md) that has a PostgreSQL interface and SQL is supported. You can read more documentation [here](https://www.vidardb.com/docs/). 

VidarDB is actively developed and maintained by VidarDB team. Feel free to report bugs or issues via Github. If you are interested in contributing to VidarDB, see [CONTRIBUTING.md](./CONTRIBUTING.md).

### For users, CMake provides the ability of compilation and installation across various platforms (Linux, Win, MacOS) with minimum lib size. 

```
sudo make install
```

### For engine developers, Makefile provides debug info with much bigger lib size, workable in linux.

- For static library:

    ```shell
    sudo DEBUG_LEVEL=0 make static_lib install-static -j[cores]
    ```

- For shared library:

    ```shell
    sudo DEBUG_LEVEL=0 make shared_lib install-shared -j[cores]
    ```

If you like **vidardb-engine**, please leave us a star. ![GitHub stars](https://img.shields.io/github/stars/vidardb/vidardb-engine?style=social)
