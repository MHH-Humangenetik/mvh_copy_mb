---
inclusion: always
---

# Project Setup and Standards

## Package Manager

This project uses **uv** for all Python package management and command execution.

Always use:
- `uv run <command>` for running Python scripts and CLI commands
- `uv sync` for syncing dependencies from `pyproject.toml` (with `--dev` for dev dependencies)
- `uv add <package>` for adding new dependencies to the project
- `uv add --dev <package>` for adding new dev dependencies to the project

Never use `pip`, `poetry`, or other package managers directly.

## Testing

Always run tests before commiting. Use `uv run pytest` for running tests.

## Git

For each spec create a separate branch. The branch name should be the spec name.

When you finish a task, make a commit. Use conventional commits. Keep the commit message short. Only more than one sentence when absolutely necessary. Check if you are on the correct branch. Only commit the files you changed yourself.

## Settings

Setting for all runnable scripts should be definable by a .env file or via command line parameter.

## Documentation

Do not crete Markdown summaries while debugging. Only append to README.md if new funtionality is introduced.