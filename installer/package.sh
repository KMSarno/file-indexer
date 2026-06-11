#!/usr/bin/env bash
# Build FileIndexer-installer.zip: the programs + manifests + installer assets.
set -euo pipefail

DIST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$DIST_DIR/.." && pwd)"
STAGE="$DIST_DIR/FileIndexer-installer"
ZIP="$DIST_DIR/FileIndexer-installer.zip"

rm -rf "$STAGE" "$ZIP"
mkdir -p "$STAGE"

# Programs + dependency manifests from the project root.
for f in crawler.py query_app.py compact_db.py pyproject.toml uv.lock; do
  cp "$ROOT/$f" "$STAGE/"
done

# Installer assets from dist/.
cp "$DIST_DIR/install.sh" "$STAGE/"
cp "$DIST_DIR/com.fileindexer.queryapp.plist.template" "$STAGE/"
cp "$DIST_DIR/README.md" "$STAGE/"
chmod +x "$STAGE/install.sh"

( cd "$DIST_DIR" && zip -r -q "$(basename "$ZIP")" "$(basename "$STAGE")" )
rm -rf "$STAGE"

echo "Built: $ZIP"
unzip -l "$ZIP"
