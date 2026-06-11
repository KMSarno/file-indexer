# File Indexer Tester Build

This is a local desktop test build for macOS. It starts a Python backend inside
the app and opens the File Indexer interface in a native window.

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
2. Drag **File Indexer** to **Applications**.
3. Open **File Indexer**.

If macOS blocks the unsigned test build, right-click the app and choose
**Open**, then confirm.

## First Run

The app uses its own database under your macOS Application Support folder.
On first launch, `uv` creates a private Python environment there for the backend.
Click **Scan for new** to create the first index. The first scan can take hours
on a large machine or external drives.

Use **Edit exclude list** before scanning to skip drives or folders you do not
want indexed.
