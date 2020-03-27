# Contributing to VidarDB

## Clang-format installation

We are currently using clang-format-10 to keep our code style consistent.

Please add the corresponding source to the end of `/etc/apt/sources.list`:

 - For Ubuntu 16.04: 

    ```
    deb http://apt.llvm.org/xenial/ llvm-toolchain-xenial-10 main
    deb-src http://apt.llvm.org/xenial/ llvm-toolchain-xenial-10 main
    ```

 - For Ubuntu 18.04:

    ```
    deb http://apt.llvm.org/bionic/ llvm-toolchain-bionic-10 main
    deb-src http://apt.llvm.org/bionic/ llvm-toolchain-bionic-10 main
    ```

 - For Ubuntu 19.04 (eoan):

    ```
    deb http://apt.llvm.org/eoan/ llvm-toolchain-eoan-10 main
    deb-src http://apt.llvm.org/eoan/ llvm-toolchain-eoan-10 main
    ```

Then, run the following commands to install `clang-format-10`:

```bash
sudo apt remove clang-format

wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | sudo apt-key add -

sudo apt update

sudo apt install clang-format-10
```

Finally, rename `clang-format-10` to `clang-format`:

```bash
sudo mv /usr/bin/clang-format-10 /usr/bin/clang-format
```

## Pre-commit hook installation

We add [pre-commit hook](https://github.com/barisione/clang-format-hooks) to check the style of changed code before every commit. 
Before opening a pull request, it is better for a developer to register the pre-commit hook:

```bash
make hook_install
```

Then, every time you commit your code, the hook will check if your code matches the coding standard.
If it doesn't, you can choose to fix it automatically or manually.

Note: if you use git through a GUI or some other tool (i.e. not directly on the command line), the script will fail to
get your input. In this case disable the interactive behaviour with:
 
 ```bash
git config hooks.clangFormatDiffInteractive false
```
