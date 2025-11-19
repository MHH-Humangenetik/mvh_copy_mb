# mvh-copy-mb

Tool to process MVH Meldebestaetigung CSV files and organize them based on metadata, resolving pseudonyms via gPAS.

## Setup

1. Install dependencies:
   ```bash
   uv sync
   ```

2. Create a `.env` file based on `.env.example`:
   ```bash
   cp .env.example .env
   ```
   Edit `.env` with your configuration.

## Usage

Run the script:

```bash
uv run mvh_copy_mb --help
```
