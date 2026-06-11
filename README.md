<p align="center">
  <img src="docs/screenshots/app-icon.png" width="120" alt="Kendex icon">
</p>

<h1 align="center">Kendex</h1>

<p align="center"><b>A local file-index console for macOS.</b><br>
It walks your drives, records rich metadata (size, MD5, MIME type, EXIF for
photos/videos) for every file into a DuckDB database, and gives you a fast
desktop UI for querying it — including a duplicate-file manager.</p>

Everything runs locally: the index never leaves your machine, queries run
against a read-only connection, and maintenance runs work on a disposable copy
of the database that is swapped in atomically only when a run succeeds — an
in-progress crawl can always be halted and discarded without touching your
current index.

## Screenshots

![The Kendex console: query results with the row inspector open](docs/screenshots/light-mode.png)

| A scan in progress | Open from the index | Duplicate manager |
|---|---|---|
| ![Indexing with live file count and rate](docs/screenshots/scan-progress.png) | ![Right-click menu: Open, Quick Look, Reveal, Copy path](docs/screenshots/open-menu.png) | ![Duplicate manager with copies marked for deletion](docs/screenshots/duplicate-manager.png) |

## The desktop app

The Electron app (`Kendex.app`) starts the Python backend on a free localhost
port and opens it in a native window. It keeps its own database under
`~/Library/Application Support/kendex/files.db`, so it never interferes with a
browser-mode service or another crawl.

### Requirements

- **macOS** (Apple Silicon for the prebuilt artifacts)
- **[uv](https://docs.astral.sh/uv/)** — manages Python and dependencies:
  `curl -LsSf https://astral.sh/uv/install.sh | sh`
- **libmagic** (required): `brew install libmagic`
- **exiftool** (optional, for photo/video metadata): `brew install exiftool`

### Run from source

```bash
npm install
npm start
```

### Package a build

```bash
npm run check   # syntax checks
npm run smoke   # boots the backend through Electron and exits
npm run dist    # DMG + ZIP in dist/
```

Builds are ad-hoc signed (an `afterPack` hook re-signs the bundle), so
Gatekeeper shows the one-time *unverified developer* flow — **System Settings →
Privacy & Security → Open Anyway** — rather than a dead-end "damaged" error.
Tester install steps, including how to seed the app with an existing
`files.db`, live in [README-DAD-TESTER.md](README-DAD-TESTER.md).

To point the app at a specific database instead of its own:

```bash
FILE_INDEXER_DB="$HOME/FileIndexer/files.db" npm start
```

### What the console gives you

- **Locate form + SQL box** — build a search from name/extension/date/volume
  fields, or write DuckDB SQL directly (Cmd+Enter runs; Cmd+↑/↓ recalls
  history; *Save query* pins your own presets to the sidebar).
- **Readable results** — human-readable sizes, sortable columns, a type-to
  filter, CSV export, and a row inspector with every column of the selected
  file.
- **Open from the index** — right-click any result (or use the inspector):
  Open, Quick Look, Reveal in Finder, Copy path. Space previews the selected
  row; Enter opens it.
- **Duplicate manager** — top duplicate groups by wasted space; mark copies to
  delete (at least one copy of each file must stay) and export a reviewed path
  list for `rm`/`xargs`. Kendex itself never deletes files.
- **Index maintenance** — scan for new files, refresh changed, prune deleted,
  prune excluded, full sync, and DB compaction, all against a disposable copy
  with live progress (file count and rate during first scans, a true
  percentage when the run has a known total). The app holds off system sleep
  while a run is active.
- **Status at a glance** — file count, indexed bytes, volume online/offline
  state, and index age above the form; rows on unmounted volumes are dimmed.
- **Dark and light themes** — follows the system by default; the toggle in the
  sidebar footer remembers your choice.

## Browser mode (headless service)

The same backend can run as a LaunchAgent serving <http://127.0.0.1:8800>:

```bash
./install.sh
```

The installer asks for an install directory and database path, installs
dependencies, and registers the LaunchAgent. The crawler can also be driven
directly from a shell (`FILE_INDEXER_DB` selects the database):

```bash
uv run crawler.py                      # add new files (resumes; skips indexed)
uv run crawler.py --reindex-changed    # refresh changed files
uv run crawler.py --prune              # drop rows for deleted files
uv run crawler.py --prune-excluded     # drop rows now covered by excludes
```

After adding directories to the exclude list, run **Prune excluded** to remove
already-indexed rows under them, then **Compact DB** to reclaim the space.

## Notes

- Timestamps are **stored in UTC** and **displayed in your local time**.
- The backend binds to `127.0.0.1` only and rejects cross-origin requests; the
  query connection is read-only, so queries can never modify the index.
- Exclude lists are path prefixes, one per line, edited in-app (**Edit exclude
  list**); changes apply on the next crawl.

## Uninstall

Desktop app: delete `Kendex.app` and `~/Library/Application Support/kendex/`.

Browser mode:

```bash
launchctl bootout gui/$(id -u)/com.fileindexer.queryapp
rm ~/Library/LaunchAgents/com.fileindexer.queryapp.plist
rm -rf <install-dir>        # also removes the database if it lives there
```
