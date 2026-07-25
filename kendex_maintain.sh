#!/bin/bash
#
# kendex_maintain.sh — unattended Kendex index maintenance, run from cron.
#
# Does what the UI's manual routine does, with no UI:
#   1. add new files      crawler.py --no-hash --roots <mounted target volumes>
#   2. update dedup       crawler.py --hash-dupes        (DB-wide, incremental)
#   3. prune deleted      crawler.py --prune             (offline volumes skipped)
#   4. compact            compact_db.py -> atomic swap into place
#
# Works directly on the live DB, so it refuses to start while anything else
# holds it: if the Kendex.app backend (query_app.py) is running, or the DB
# write lock can't be taken, the run is skipped ("OK skipped" status) and the
# next scheduled run tries again. The lock probe doubles as WAL recovery —
# opening the DB read-write replays any leftover .wal from a crash.
#
# Logging/status follow the KMSCron conventions (logs/<label>.log on the
# backup drive, OK/FAIL status file on the boot volume). The morning
# check-backups.sh only watches backup.sh jobs, so this job's status file is
# informational only — no false "stale" alerts on days it doesn't run.
#
# Scheduled from /Volumes/TB5_DOCK8/KMSCron/crontab: Mon & Thu 03:00, an hour
# after the 02:00 files.db backup so the two never overlap.

set -uo pipefail

REPO="/Volumes/TB5_DOCK8/KMSDB_PROJ/File_Indexer"
DB="/Volumes/TB5_DOCK8/KMSDB_PROJ/files.db"
ROOTS=("/Volumes/TB5_DOCK8" "/Volumes/PROJECTS" "/Volumes/DATAVOL")
UV="$HOME/.local/bin/uv"

KMSCRON="/Volumes/TB5_DOCK8/KMSCron"
LABEL="kendex-maintain"
LOG="$KMSCRON/logs/$LABEL.log"
LOG_MAX_LINES=3000
STATUS_DIR="$HOME/Library/Application Support/KMSCron/status"
STATUS="$STATUS_DIR/$LABEL.status"

mkdir -p "$STATUS_DIR" "$KMSCRON/logs"

ts()  { date '+%Y-%m-%d %H:%M:%S'; }
say() { echo "[$(ts)] $*" >> "$LOG"; }

SKIPPED=""
record_status() {
  local code=$?
  if [ "$code" -eq 0 ]; then
    echo "OK ${SKIPPED}$(ts)" > "$STATUS"
  else
    echo "FAIL $(ts) exit=$code" > "$STATUS"
  fi
  # tqdm writes a line per refresh when its stream is a file; keep the log bounded.
  if [ -f "$LOG" ]; then
    tail -n "$LOG_MAX_LINES" "$LOG" > "$LOG.tmp" && mv "$LOG.tmp" "$LOG"
  fi
}
trap record_status EXIT

say "START maintenance (add/hash-dupes/prune/compact) on $DB"

# --- refuse to touch the DB while anything else holds it -------------------

if pgrep -f "query_app.py" > /dev/null 2>&1; then
  say "SKIP — Kendex is open (query_app.py running); will retry next scheduled run"
  SKIPPED="skipped "
  exit 0
fi

cd "$REPO" || exit 1

# Probe the write lock (and replay any leftover .wal from a crash).
if ! "$UV" run python -c \
    "import duckdb, sys; duckdb.connect(sys.argv[1]).close()" "$DB" >> "$LOG" 2>&1; then
  say "SKIP — could not take the DB write lock; will retry next scheduled run"
  SKIPPED="skipped "
  exit 0
fi

# --- which target volumes are actually mounted -----------------------------

roots=()
for r in "${ROOTS[@]}"; do
  if [ -d "$r" ]; then
    roots+=("$r")
  else
    say "note: $r not mounted — not scanned this run"
  fi
done
if [ "${#roots[@]}" -eq 0 ]; then
  say "SKIP — no target volumes mounted"
  SKIPPED="skipped "
  exit 0
fi

# --- the four steps --------------------------------------------------------

step() {
  say "step: $*"
  if ! "$@" >> "$LOG" 2>&1; then
    say "FAILED: $*"
    exit 1
  fi
}

step "$UV" run crawler.py --no-hash --roots "${roots[@]}"
step "$UV" run crawler.py --hash-dupes
step "$UV" run crawler.py --prune

TMP="$DB.compacting"
rm -f "$TMP" "$TMP.wal"
step "$UV" run compact_db.py "$DB" "$TMP"
rm -f "$DB.wal"            # none expected after a clean close; never leave a
mv -f "$TMP" "$DB"         # stale one to replay against the fresh file
say "DONE — $(stat -f %z "$DB" | awk '{printf "%.2f GB", $1/1e9}') after compact"
