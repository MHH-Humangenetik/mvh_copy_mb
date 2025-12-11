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

### Processing CSV Files

Process Meldebestaetigung CSV files and store them in the database:

```bash
uv run mvh_copy_mb --help
```

### Web Interface

Start the web server to view and manage processed records:

```bash
uv run web
```

The web interface will be available at `http://localhost:8000` by default.

#### Web Server Configuration

You can configure the web server using command-line options or environment variables:

```bash
# Custom host and port
uv run web --host 0.0.0.0 --port 8080

# Enable auto-reload for development
uv run web --reload

# Set log level
uv run web --log-level debug
```

Or use environment variables in your `.env` file:

```bash
WEB_HOST=0.0.0.0
WEB_PORT=8000
WEB_RELOAD=false
WEB_LOG_LEVEL=info
DB_PATH=./data/meldebestaetigungen.duckdb
```

#### Web Interface Features

The web interface provides:

- **Table View**: Display all Meldebestätigungen with key metadata
- **Pair Grouping**: Genomic and clinical records for the same Case ID are visually grouped
- **Priority Sorting**: Records are sorted by priority (complete pairs not done, incomplete pairs, complete pairs done)
- **Filtering**: Real-time client-side filtering across all columns
- **Sorting**: Click column headers to sort by that column
- **Done Status**: Mark complete pairs as "done" with persistent checkboxes
- **Rosé Pine Theme**: Beautiful color scheme with automatic dark mode support

#### Database Path

By default, the web server looks for the database at `./data/meldebestaetigungen.duckdb`. You can change this by setting the `DB_PATH` environment variable in your `.env` file.

## Multi-User Synchronization

The system supports real-time multi-user collaboration with WebSocket-based synchronization. Multiple users can work simultaneously on different records with automatic conflict resolution and real-time updates.

### Features

- **Real-time Updates**: Changes are instantly synchronized across all connected users
- **Optimistic Locking**: Prevents conflicts when multiple users edit the same record
- **Automatic Reconnection**: Handles network interruptions gracefully
- **Audit Trail**: Complete logging of all multi-user interactions
- **Performance Optimized**: Efficient batching and connection pooling

### Quick Start

For development:
```bash
# Start with multi-user sync enabled (default)
uv run web
```

## Requirements

- Python 3.11+
- Modern web browser with WebSocket support
- Network access to gPAS service endpoint
