# File Indexer

A single-host filesystem indexer for macOS. It walks your drives, records rich
metadata (size, MD5, MIME type, EXIF for photos/videos) for every file into a
DuckDB database, and serves a local web UI for querying it — including a
duplicate-file finder. Designed to resume after interruption and to keep the
index current with incremental refresh / prune passes.

## Requirements

- **macOS**
- **[uv](https://docs.astral.sh/uv/)** — manages Python 3.14 and dependencies:
  `curl -LsSf https://astral.sh/uv/install.sh | sh`
- **libmagic** (required): `brew install libmagic`
- **exiftool** (optional, for photo/video metadata): `brew install exiftool`

## Install

```bash
unzip FileIndexer-installer.zip
cd FileIndexer-installer
./install.sh
```

The installer asks for an install directory and a database path, installs
dependencies, and sets up a LaunchAgent so the query UI (http://127.0.0.1:8800)
starts now and at every login. It’s safe to re-run to upgrade the programs;
your database and exclude list are left untouched.

## First crawl

1. Open <http://127.0.0.1:8800>, click **Edit exclude list**, and add any
   volumes you don’t want indexed (Time Machine, scratch disks, backups).
2. Run the first full crawl (can take hours for a large collection). **Open a
   new terminal first** so `FILE_INDEXER_DB` (added to your `~/.zshrc` by the
   installer) is loaded:
   ```bash
   cd <install-dir>
   uv run crawler.py
   ```
   (Or, in any shell, set it inline: `FILE_INDEXER_DB="<your-db-path>" uv run crawler.py`)
3. Keep it current later:
   ```bash
   uv run crawler.py --reindex-changed   # refresh changed files
   uv run crawler.py                      # add new files (resumes; skips indexed)
   uv run crawler.py --prune              # drop rows for deleted files
   uv run crawler.py --prune-excluded     # drop rows now covered by excludes
   ```

   After adding directories to the exclude list, run `--prune-excluded` to
   remove already-indexed rows under them, then **Compact DB** to reclaim the
   space. The web and desktop UI expose the same operation as **Prune
   excluded**.

The database location comes from the `FILE_INDEXER_DB` environment variable
(the installer sets it in your `~/.zshrc` and in the LaunchAgent). The web UI’s
Maintenance panel can run all of the above against a disposable copy without
leaving the browser.

## Desktop app

The Electron wrapper runs the same Python query app in the background and opens
it in a native desktop window, so you do not have to manage a browser tab.

```bash
npm install
npm start
```

The desktop app starts the Python backend with `uv run query_app.py` on a free
localhost port, then shuts it down when the app quits. By default it uses an
isolated Electron app-data database, so it will not interfere with the
LaunchAgent/browser service or an in-progress crawl at `~/FileIndexer/files.db`.
To point the desktop app at a specific database, set `FILE_INDEXER_DB`:

```bash
FILE_INDEXER_DB="$HOME/FileIndexer/files.db" npm start
```

This is currently a development wrapper, not a fully bundled distributable. A
later packaging pass should bundle Python dependencies, handle `libmagic` and
`exiftool`, and produce a signed/notarized macOS `.app`.

## Notes

- All timestamps are **stored in UTC** and **displayed in your local time**.
- The query UI is bound to `127.0.0.1` only and rejects cross-origin requests;
  it holds a read-only DB connection, so queries can never modify the index.
- `cleanup_tm.py` (a one-off Time Machine snapshot pruner) is **not** included —
  it isn’t needed for normal use.

## Uninstall

```bash
launchctl bootout gui/$(id -u)/com.fileindexer.queryapp
rm ~/Library/LaunchAgents/com.fileindexer.queryapp.plist
rm -rf <install-dir>        # also removes the database if it lives there
```
