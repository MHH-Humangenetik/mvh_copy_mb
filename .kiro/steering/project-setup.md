---
inclusion: always
---

# Project Setup and Standards

## Package Manager

This project uses **uv** for all Python package management and command execution.

Always use:
- `uv run <command>` for running Python scripts and CLI commands
- `uv sync` for syncing dependencies from `pyproject.toml`
- `uv add <package>` for adding new dependencies to the project

Never use `pip`, `poetry`, or other package managers directly.

## Git

For each spec create a separate branch. The branch name should be the spec name.

When you finish a task, make a commit. Use conventional commits. Keep the commit message short. Only more than one sentence when absolutely necessary. Check if you are on the correct branch.