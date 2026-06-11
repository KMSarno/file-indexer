#!/usr/bin/env bash
#
# File Indexer installer (macOS).
#   * copies the programs into an install directory
#   * installs Python deps with uv (downloads Python 3.14 on first run)
#   * installs a LaunchAgent so the query UI starts now and at every login
#
# Re-runnable: it overwrites the install dir's program files and reloads the
# agent. Your database and exclude list are left untouched.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LABEL="com.fileindexer.queryapp"
PLIST_DEST="$HOME/Library/LaunchAgents/$LABEL.plist"
LOG_PATH="$HOME/Library/Logs/fileindexer-queryapp.log"

say() { printf '\n\033[1m%s\033[0m\n' "$*"; }
err() { printf '\033[31m%s\033[0m\n' "$*" >&2; }

say "File Indexer installer"

# ---- prerequisites --------------------------------------------------------
if ! command -v uv >/dev/null 2>&1; then
  err "uv is required but not found. Install it, then re-run this script:"
  err "    curl -LsSf https://astral.sh/uv/install.sh | sh"
  exit 1
fi
echo "  uv:       $(command -v uv)"

# libmagic is REQUIRED -- crawler.py does a hard 'import magic' (python-magic
# loads the libmagic dylib via ctypes at import time).
if   [ -e /opt/homebrew/lib/libmagic.dylib ] \
  || [ -e /usr/local/lib/libmagic.dylib ] \
  || ls /opt/homebrew/Cellar/libmagic/*/lib/libmagic*.dylib >/dev/null 2>&1 \
  || ls /usr/local/Cellar/libmagic/*/lib/libmagic*.dylib   >/dev/null 2>&1; then
  echo "  libmagic: found"
else
  err "libmagic not found -- it is REQUIRED. Install it, then re-run:"
  err "    brew install libmagic"
  exit 1
fi

if command -v exiftool >/dev/null 2>&1; then
  echo "  exiftool: $(command -v exiftool)"
else
  echo "  exiftool: not found (optional -- EXIF metadata is skipped without it)"
  echo "            install with:  brew install exiftool"
fi

# ---- prompts --------------------------------------------------------------
echo
read -r -p "Install directory [$HOME/FileIndexer]: " INSTALL_DIR
INSTALL_DIR="${INSTALL_DIR:-$HOME/FileIndexer}"
read -r -p "Database file path [$INSTALL_DIR/files.db]: " DB_PATH
DB_PATH="${DB_PATH:-$INSTALL_DIR/files.db}"

# ---- copy programs --------------------------------------------------------
say "Installing programs to $INSTALL_DIR"
mkdir -p "$INSTALL_DIR" "$(dirname "$DB_PATH")"
for f in crawler.py query_app.py compact_db.py pyproject.toml uv.lock; do
  cp "$SCRIPT_DIR/$f" "$INSTALL_DIR/"
done

# ---- dependencies ---------------------------------------------------------
say "Installing dependencies (uv sync) -- first run may download Python 3.14"
( cd "$INSTALL_DIR" && uv sync )

# Now that the venv exists, prove libmagic actually loads (catches a present-
# but-broken install before the first crawl does).
if ! ( cd "$INSTALL_DIR" && uv run python -c "import magic" ) >/dev/null 2>&1; then
  err "Python could not load libmagic. Run 'brew install libmagic' and re-run."
  exit 1
fi

# ---- LaunchAgent ----------------------------------------------------------
say "Installing the login LaunchAgent"
mkdir -p "$HOME/Library/LaunchAgents" "$(dirname "$LOG_PATH")"
UV_BIN="$(command -v uv)"
sed -e "s|@@UV@@|$UV_BIN|g" \
    -e "s|@@INSTALL_DIR@@|$INSTALL_DIR|g" \
    -e "s|@@DB_PATH@@|$DB_PATH|g" \
    -e "s|@@LOG@@|$LOG_PATH|g" \
    "$SCRIPT_DIR/com.fileindexer.queryapp.plist.template" > "$PLIST_DEST"

launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$PLIST_DEST"

# ---- shell env for manual CLI crawls --------------------------------------
# The LaunchAgent passes FILE_INDEXER_DB to the server, but `uv run crawler.py`
# from a terminal needs it too, so add it to the shell rc once.
RC="$HOME/.zshrc"
if [ -f "$RC" ] && grep -q "FILE_INDEXER_DB" "$RC"; then
  echo "  (FILE_INDEXER_DB already present in $RC -- left as-is)"
else
  printf '\n# File Indexer DB location (used by crawler.py CLI runs)\nexport FILE_INDEXER_DB="%s"\n' "$DB_PATH" >> "$RC"
  echo "  Added FILE_INDEXER_DB to $RC"
fi

# ---- done -----------------------------------------------------------------
say "Done."
cat <<EOF
  Query UI:  http://127.0.0.1:8800   (running now; starts again at every login)
  Programs:  $INSTALL_DIR
  Database:  $DB_PATH

  Next steps:
    1. Open http://127.0.0.1:8800 and click "Edit exclude list" to skip
       volumes you don't want indexed (Time Machine, scratch disks, etc.)
       BEFORE the first crawl.
    2. Run the first full crawl (can take hours for a large collection):
         cd "$INSTALL_DIR" && FILE_INDEXER_DB="$DB_PATH" uv run crawler.py
       (new terminals will have FILE_INDEXER_DB set automatically)
    3. Keep it current later with:
         uv run crawler.py --reindex-changed   # refresh modified files
         uv run crawler.py                      # add new files
         uv run crawler.py --prune              # drop deleted files

  Uninstall:
    launchctl bootout gui/\$(id -u)/$LABEL
    rm "$PLIST_DEST"
    rm -rf "$INSTALL_DIR"        # also removes the database if it lives here
EOF
