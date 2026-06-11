# Kendex Tester Build

This is a local desktop test build for macOS. It starts a Python backend inside
the app and opens the Kendex interface in a native window.

## Before Opening

Install the required command-line dependencies once:

```bash
brew install uv libmagic
```

Optional, for richer photo/video metadata:

```bash
brew install exiftool
```

## Install

1. Open the `.dmg`.
2. Drag **Kendex** to **Applications**.
3. Open **Kendex** — it just opens. (Recent builds are signed and notarized
   by Apple, so there's no security warning.)

Once it's running, Kendex keeps itself up to date: when a new version is
released it downloads quietly in the background and installs the next time you
quit, so you rarely need a new download again.

<details>
<summary>Older unsigned test build?</summary>

If you were sent an early build and macOS says it "cannot be verified" or is
"damaged": try to open it once, then go to **System Settings → Privacy &
Security → Open Anyway**. If that doesn't work, run this in Terminal and reopen:

```bash
xattr -cr /Applications/Kendex.app
```
</details>

## First Run

The app uses its own database under your macOS Application Support folder.
On first launch, `uv` creates a private Python environment there for the backend.
Click **Scan for new** to create the first index. The first scan can take hours
on a large machine or external drives.

Use **Edit exclude list** before scanning to skip drives or folders you do not
want indexed.

If you add new excludes after a scan, run **Prune excluded** to remove matching
rows from the existing index, then run **Compact DB** to shrink the database.

## Use an Existing Database (skip the first scan)

If you already have a `files.db` from the browser-based indexer, you can reuse
a copy of it instead of waiting hours for a first scan:

1. Open Kendex once, then choose **File → Open App Data Folder**.
2. Quit Kendex.
3. Copy your existing database into that folder, named exactly `files.db`.
4. Reopen Kendex — the index is queryable immediately.

Kendex opens this file read-only for queries, and maintenance runs work on a
disposable copy that is swapped in only on success — your original database
(wherever you copied it from) is never touched.

## Store the Database on Another Volume

By default the index lives in the app's Application Support folder on your boot
drive. A large index is better kept on an external/data volume. To relocate it:

1. Choose **File → Choose Database Location…** and pick a folder (on any
   mounted volume).
2. Click **Relaunch Now** when prompted.

Kendex remembers the choice (in `config.json` in its app data folder) and uses
it on every launch. Relaunching starts a **fresh** index at the new location —
to reuse an existing one, quit first and copy your `files.db` into the chosen
folder. (If the app was launched with the `FILE_INDEXER_DB` environment variable
set, that variable wins and the menu choice is saved but not applied.)
