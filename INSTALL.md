## Compilation

**Important**: If you plan to run VidarDB in production, don't compile using default 
`make` or `make all`. That will compile VidarDB in debug mode, which is much slower
than release mode.

VidarDB's library should be able to compile without any dependency installed,
although we recommend installing some compression libraries (see below).
We do depend on newer gcc/clang with C++11 support.

There are few options when compiling VidarDB:

* [recommended] `make static_lib` will compile libvidardb.a, VidarDB static library. Compiles static library in release mode.

* `make shared_lib` will compile libvidardb.so, VidarDB shared library. Compiles shared library in release mode.

* `make check` will compile and run all the unit tests. `make check` will compile VidarDB in debug mode.

* `make all` will compile our static library, and all our tools and unit tests. Our tools
depend on gflags. You will need to have gflags installed to run `make all`. This will compile VidarDB in debug mode. Don't
use binaries compiled by `make all` in production.

* By default the binary we produce is optimized for the platform you're compiling on
(-march=native or the equivalent). If you want to build a portable binary, add 'PORTABLE=1' before
your make commands, like this: `PORTABLE=1 make static_lib`

## Dependencies

* You can link VidarDB with following compression libraries:
  - [zlib](http://www.zlib.net/) - a library for data compression.
  - [snappy](https://code.google.com/p/snappy/) - a library for fast
      data compression.

## Supported platforms

* **Linux - Ubuntu**
    * Upgrade your gcc to version at least 4.7 to get C++11 support.
    * Install snappy. This is usually as easy as:
      `sudo apt-get install libsnappy-dev`.
    * Install zlib. Try: `sudo apt-get install zlib1g-dev`.
* **Linux - CentOS**
    * Upgrade your gcc to version at least 4.7 to get C++11 support:
      `yum install gcc47-c++`

    * Install snappy:

              wget https://snappy.googlecode.com/files/snappy-1.1.1.tar.gz
              tar -xzvf snappy-1.1.1.tar.gz
              cd snappy-1.1.1
              ./configure && make && sudo make install

    * Install zlib:

              sudo yum install zlib
              sudo yum install zlib-devel

* **OS X**:
    * Install latest C++ compiler that supports C++ 11:
        * Update XCode:  run `xcode-select --install` (or install it from XCode App's settting).
        * Install via [homebrew](http://brew.sh/).
            * If you're first time developer in MacOS, you still need to run: `xcode-select --install` in your command line.
            * run `brew tap homebrew/versions; brew install gcc47 --use-llvm` to install gcc 4.7 (or higher).
    * run `brew install vidardb`

* **iOS**:
  * Run: `TARGET_OS=IOS make static_lib`. When building the project which uses vidardb iOS library, make sure to define two important pre-processing macros: `VIDARDB_LITE` and `IOS_CROSS_COMPILE`.

* **Windows**:
  * For building with MS Visual Studio 13 you will need Update 4 installed.
  * Read and follow the instructions at CMakeLists.txt
