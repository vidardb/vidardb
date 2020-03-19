# Contributing to VidarDB

## Code Style

We are currently using clang-format to keep our code style consistent.
We add [pre-commit hook](https://github.com/barisione/clang-format-hooks) to check the style of changed code before every commit. 
Before opening a pull request, it is better for a developer to register the pre-commit hook:

```bash
./scripts/git-pre-commit-format install
```

Then, every time you commit your code, the hook will check if your code matches the coding standard.
If it doesn't, you can choose to fix it automatically or manually.
