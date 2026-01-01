# Duplicate File Finder (Rust + SQLite)

Finds duplicate files by hashing contents with XxHash3_64 and stores results in a local SQLite database. The CLI can scan a directory, export duplicate records to CSV, and clean up stale DB entries.

## Features
- Fast content hashing with XxHash3_64
- SQLite storage with migrations in `migrations/`
- Skips zero-byte files
- Skips `.git` and `node_modules` directories
- Export duplicates to CSV with dynamic headers
- Configurable via TOML

## Requirements
- Rust (edition 2021)
- SQLite (via `sqlx` with embedded driver)

## Build
```bash
cargo build --release
```

## Configuration
Create a TOML config (see `config.toml.example`):
```toml
database_url = "sqlite://./dup_files.db"
search_path = "/path/to/scan"
result_output_path = "./result.csv"
```

### `config.toml.example` fields
- `database_url`  
  SQLite connection string. Example from `config.toml.example`:
  - `sqlite://test.db?mode=rwc` creates the DB file if it doesn’t exist (read/write/create).
  You can point this to any path, for example `sqlite://./dup_files.db`.
- `search_path`  
  Root directory to scan for files. This can be absolute (recommended) or relative to where you run the command.
- `result_output_path`  
  Path to write the CSV export (duplicates report). Can be relative or absolute.

## Usage
```bash
# Build and run
cargo run --release -- -c config.toml find-dups

# Or use the built binary
./target/release/dup-file-finder -c config.toml find-dups
```

### Commands
- `find-dups`  
  Scans the `search_path`, hashes files, and records them in SQLite. After the scan, exports duplicates to CSV.

- `delete-files-not-found`  
  Deletes DB records whose files no longer exist on disk, then exports duplicates to CSV.

- `export-result`  
  Exports current duplicate results to CSV without scanning.

## Output
The CSV includes all columns from the `file` table plus `file_hash.file_size` and `hash_count` (number of files with the same hash). The output path is `result_output_path` from the config file.

## How de-duplication works
The tool identifies duplicates by hashing file contents and grouping by `(file_size, hash)`:

1) Walks the directory tree rooted at `search_path` (skipping `.git` and `node_modules`).
2) For each file, reads metadata and content:
   - `file_size` and `file_modification_time` from filesystem metadata.
   - `hash` is the XxHash3_64 of the file contents.
3) Inserts or reuses a row in `file_hash` keyed by `(file_size, hash)`.
4) Inserts or updates a row in `file` keyed by `file_path` with the `hash_id`, `file_size`, and `file_modification_time`.
5) Duplicates are any `hash_id` that appears more than once in `file` (same content + size).

### Database schema (summary)
- `file_hash`  
  Stores unique `(file_size, hash)` pairs. A unique index enforces no duplicates.
- `file`  
  Stores one row per file path with its `hash_id`, `file_size`, and `file_modification_time`. A unique index enforces one row per path.
- `file_view`  
  Convenience view joining `file` and `file_hash`.

### Notes
- A file is skipped if it already exists in the DB **with the same size and modification time**.
- Changing a file’s contents or mtime causes it to be re-hashed and updated.

## Notes
- Concurrency is capped by `CONCURRENCY_LIMIT` in `src/dup_finder.rs`.
- Logs are emitted via `env_logger`. Set `RUST_LOG=debug` for more detail.
