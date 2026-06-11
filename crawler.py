#!/usr/bin/env python3
"""
File System Crawler

Crawls all specified volumes, records rich metadata for every file,
computes MD5 hashes, and stores everything in a DuckDB database.
Resumes gracefully if interrupted.

Usage:
    uv run crawler.py              # full crawl
    uv run crawler.py --no-hash   # skip hashing (fast pass, metadata only)
    uv run crawler.py --hash-only # hash files already in DB that have no hash yet
"""

import os
import sys
import time
import hashlib
import argparse
import json
import socket
import shutil
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import magic
from tqdm import tqdm

try:
    import exiftool
except Exception:
    exiftool = None

EXIFTOOL_EXECUTABLE = None
if exiftool is not None:
    for candidate in (
        shutil.which("exiftool"),
        "/opt/homebrew/bin/exiftool",
        "/usr/local/bin/exiftool",
    ):
        if candidate and os.path.exists(candidate):
            EXIFTOOL_EXECUTABLE = candidate
            break

try:
    EXIFTOOL_AVAILABLE = exiftool is not None and EXIFTOOL_EXECUTABLE is not None
except NameError:
    EXIFTOOL_AVAILABLE = False

# =============================================================================
# CONFIGURATION -- edit this section to match your setup
# =============================================================================

# Database file location. Override per-machine with the FILE_INDEXER_DB env var
# (the installer sets it); otherwise falls back to this default. The --db flag
# still overrides at runtime (used by the web UI's disposable working copy).
DB_PATH = Path(os.environ.get("FILE_INDEXER_DB", "/Volumes/TB5_DOCK8/KMSDB_PROJ/files.db"))

# Root paths to crawl. Just "/" -- os.walk descends into /Volumes (mount points
# are real directories), so a single "/" root already covers every mounted
# volume. Listing "/Volumes" as a second root would walk every external volume
# a second time, redoing MIME/MD5/EXIF on each file (the UNIQUE constraint stops
# duplicate rows, but not the wasted work). The script automatically skips the
# directory where DB_PATH lives.
CRAWL_ROOTS = [
    Path("/"),          # covers internal SSD AND everything under /Volumes
]

# Built-in exclude prefixes -- always applied, never editable from the web UI.
# User-added excludes are layered on top from exclude_paths.json (see
# load_user_excludes); EXCLUDE_PATHS below is the union of the two.
EXCLUDE_DEFAULTS = {
    "/Volumes/TM7T",
    "/Volumes/TM16T",
    "/Volumes/MACBAK7T",
    "/Volumes/BIGVFX",
    "/Volumes/FOOTAGE",
    "/Volumes/.timemachine",                  # macOS exposes each TM snapshot here -- prune all
    "/.MobileBackups",                        # legacy local TM snapshots
    "/.Spotlight-V100",
    "/.fseventsd",
    "/.DocumentRevisions-V100",
    "/proc",
    "/sys",
    "/dev",
    "/private/var/vm",                        # macOS swap
    "/private/var/folders",                   # macOS temp/cache
    "/System/Volumes/Data",                   # APFS firmlink target -- avoid double-walk
    "/System/Volumes/Preboot",                # macOS Preboot volume -- not user data
    "/Volumes/Preboot",                       # alternate mount path for Preboot
    "/System/Volumes/VM",                     # macOS swapfiles -- not user data
    "/System/Volumes/Hardware",               # recovery logs -- not user data
    "/System/Volumes/Update",                 # OS update staging -- not user data
    "/System/Volumes/xarts",                  # secure-element storage -- not user data
    "/System/Volumes/iSCPreboot",             # iBoot/recovery staging -- not user data
    "/.Trashes",
    "/private/tmp",
    "/Applications/Blackmagic RAW",           # SDK sample files ship root-only-readable -- recurring PermissionErrors
}

# User-editable extra excludes, managed by the web UI's "Edit exclude list"
# panel. Lives next to this script so both the crawler and query_app.py agree
# on its location. Each entry is an absolute path prefix.
EXCLUDE_CONFIG = Path(__file__).resolve().parent / "exclude_paths.json"


def load_user_excludes() -> set:
    """Read the user exclude list. Missing/garbage file -> no user excludes.
    Only absolute-path strings are accepted; everything else is ignored."""
    try:
        with open(EXCLUDE_CONFIG) as f:
            data = json.load(f)
    except (FileNotFoundError, ValueError, OSError):
        return set()
    if not isinstance(data, list):
        return set()
    return {p for p in data if isinstance(p, str) and p.startswith("/")}


# The effective exclude set: locked defaults plus whatever the UI saved.
EXCLUDE_PATHS = EXCLUDE_DEFAULTS | load_user_excludes()

# File extensions that get full EXIF extraction (slower but rich metadata)
EXIF_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".tif", ".tiff", ".heic", ".heif",
    ".cr2", ".cr3", ".nef", ".arw", ".raf", ".dng", ".rw2",
    ".mp4", ".mov", ".mxf", ".braw", ".avi", ".mkv", ".m4v",
    ".mp3", ".wav", ".aiff", ".flac", ".m4a",
}

# How many files to batch-insert before committing to DB
COMMIT_BATCH_SIZE = 100

# =============================================================================
# DATABASE SETUP
# Each statement is executed individually -- DuckDB does not accept
# multiple statements in a single execute() call.
# =============================================================================

SCHEMA_STATEMENTS = [
    "CREATE SEQUENCE IF NOT EXISTS seq_files",
    "CREATE SEQUENCE IF NOT EXISTS seq_crawl_log",
    "CREATE SEQUENCE IF NOT EXISTS seq_errors",
    """
    CREATE TABLE IF NOT EXISTS files (
        id                  INTEGER PRIMARY KEY DEFAULT nextval('seq_files'),
        path                TEXT NOT NULL UNIQUE,
        volume              TEXT,
        filename            TEXT,
        extension           TEXT,
        size_bytes          BIGINT,
        md5                 TEXT,
        mime_type           TEXT,
        is_symlink          BOOLEAN,
        is_hidden           BOOLEAN,
        inode               BIGINT,
        hard_link_count     BIGINT,
        created_at          TIMESTAMP,
        modified_at         TIMESTAMP,
        accessed_at         TIMESTAMP,
        indexed_at          TIMESTAMP,
        exif_camera_make    TEXT,
        exif_camera_model   TEXT,
        exif_shoot_date     TEXT,
        exif_gps_lat        DOUBLE,
        exif_gps_lon        DOUBLE,
        exif_image_width    BIGINT,
        exif_image_height   BIGINT,
        exif_duration_secs  DOUBLE,
        exif_video_codec    TEXT,
        exif_audio_codec    TEXT,
        exif_focal_length   TEXT,
        exif_aperture       TEXT,
        exif_iso            TEXT,
        exif_raw            TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_md5       ON files(md5)",
    "CREATE INDEX IF NOT EXISTS idx_extension ON files(extension)",
    "CREATE INDEX IF NOT EXISTS idx_size      ON files(size_bytes)",
    "CREATE INDEX IF NOT EXISTS idx_volume    ON files(volume)",
    "CREATE INDEX IF NOT EXISTS idx_modified  ON files(modified_at)",
    "CREATE INDEX IF NOT EXISTS idx_filename  ON files(filename)",
    """
    CREATE TABLE IF NOT EXISTS crawl_log (
        id           INTEGER PRIMARY KEY DEFAULT nextval('seq_crawl_log'),
        started_at   TIMESTAMP,
        finished_at  TIMESTAMP,
        host         TEXT,
        files_found  INTEGER,
        files_hashed INTEGER,
        errors       INTEGER,
        notes        TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS errors (
        id           INTEGER PRIMARY KEY DEFAULT nextval('seq_errors'),
        path         TEXT,
        error_type   TEXT,
        message      TEXT,
        occurred_at  TIMESTAMP
    )
    """,
]

# =============================================================================
# HELPERS
# =============================================================================

def init_db(con):
    """Execute each schema statement individually."""
    for stmt in SCHEMA_STATEMENTS:
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)


def get_volume(path: Path) -> str:
    """Return the mount point name for a given path."""
    try:
        p = path.resolve()
        while not os.path.ismount(p):
            p = p.parent
        name = p.name or str(p)
        return name if name else str(p)
    except Exception:
        return "unknown"


def ts_to_dt(ts: float):
    # Naive datetime representing UTC. DuckDB's TIMESTAMP has no time zone, and
    # binding a tz-AWARE datetime makes the driver silently convert it to local
    # time before stripping the zone -- which then reads back wrong and made
    # --reindex-changed treat every file as modified. Storing naive-UTC round-
    # trips exactly. All displayed times are therefore UTC.
    #
    # A corrupt/sentinel stat timestamp (out of the platform's time_t range)
    # raises OverflowError/OSError/ValueError -- and OverflowError is NOT an
    # OSError, so it escapes the per-file handlers and aborts the whole os.walk.
    # Return None (stored as NULL) instead so one bad file can't truncate a crawl.
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None)
    except (OverflowError, OSError, ValueError):
        return None


def now_utc() -> datetime:
    """Naive-UTC 'now', for the same DuckDB-TIMESTAMP reason as ts_to_dt."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def make_readout(pbar, heartbeat: float = 2.5):
    """Return show(path_str, dt) that writes a single self-overwriting line into
    the tqdm bar's postfix (which the web UI routes to its path pane):
      * a file whose work took >=1s is shown immediately as 'slow Ns: <path>'
      * otherwise the current path is sampled at most every `heartbeat` seconds,
        so fast stretches still show roughly where the crawl is.
    """
    state = {"last": time.monotonic()}

    def show(path_str: str, dt: float = 0.0):
        now = time.monotonic()
        if dt >= 1.0:
            pbar.set_postfix_str(f"slow {dt:.0f}s: {path_str}", refresh=False)
            state["last"] = now
        elif now - state["last"] >= heartbeat:
            pbar.set_postfix_str(path_str, refresh=False)
            state["last"] = now

    return show


def md5_file(path: Path) -> str | None:
    """Compute MD5 of a file. Returns None on any read error."""
    h = hashlib.md5()
    try:
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()
    except (PermissionError, OSError):
        return None


def get_mime(path: Path) -> str | None:
    try:
        return magic.from_file(str(path), mime=True)
    except Exception:
        return None


def get_exif(et_ctx, path_str: str) -> dict:
    """Extract EXIF metadata using ExifTool. Returns empty dict on failure."""
    try:
        meta_list = et_ctx.get_metadata(path_str)
        if not meta_list:
            return {}
        meta = meta_list[0]

        def g(*keys):
            for k in keys:
                v = meta.get(k)
                if v is not None:
                    return v
            return None

        lat = g("Composite:GPSLatitude", "GPS:GPSLatitude")
        lon = g("Composite:GPSLongitude", "GPS:GPSLongitude")

        dur_raw = g("QuickTime:Duration", "Duration")
        dur_secs = None
        if isinstance(dur_raw, (int, float)):
            dur_secs = float(dur_raw)
        elif isinstance(dur_raw, str) and ":" in dur_raw:
            try:
                parts = dur_raw.split(":")
                dur_secs = sum(float(x) * 60 ** i for i, x in enumerate(reversed(parts)))
            except ValueError:
                pass

        return {
            "exif_camera_make":   g("EXIF:Make", "Make"),
            "exif_camera_model":  g("EXIF:Model", "Model"),
            "exif_shoot_date":    g("EXIF:DateTimeOriginal", "DateTimeOriginal", "CreateDate"),
            "exif_gps_lat":       float(lat) if lat is not None else None,
            "exif_gps_lon":       float(lon) if lon is not None else None,
            "exif_image_width":   g("EXIF:ImageWidth", "ImageWidth"),
            "exif_image_height":  g("EXIF:ImageHeight", "ImageHeight"),
            "exif_duration_secs": dur_secs,
            "exif_video_codec":   g("QuickTime:VideoCodec", "VideoCodec"),
            "exif_audio_codec":   g("QuickTime:AudioCodec", "AudioCodec"),
            "exif_focal_length":  g("EXIF:FocalLength", "FocalLength"),
            "exif_aperture":      g("EXIF:FNumber", "Aperture"),
            "exif_iso":           g("EXIF:ISO", "ISO"),
            "exif_raw":           json.dumps({k: str(v) for k, v in meta.items() if k != "SourceFile"}),
        }
    except Exception:
        return {}


def should_skip(path: Path, skip_dirs: set) -> bool:
    """Return True if this path should be excluded from crawling. Both the
    self-exclusion dirs and EXCLUDE_PATHS match on a full path component (exact,
    or a '/'-delimited prefix) so e.g. '/x/KMSDB_PROJ' never shadows a sibling
    '/x/KMSDB_PROJ_BACKUP'."""
    path_str = str(path)
    for d in skip_dirs:
        if path_str == d or path_str.startswith(d + "/"):
            return True
    for ex in EXCLUDE_PATHS:
        if path_str == ex or path_str.startswith(ex + "/"):
            return True
    return False


# =============================================================================
# MAIN CRAWL
# =============================================================================

def crawl(do_hash: bool = True, hash_only: bool = False):
    print(f"\n{'='*60}")
    print(f"  File System Crawler")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Database: {DB_PATH}")
    print(f"  Hashing: {'yes' if do_hash else 'no'}")
    print(f"{'='*60}\n")

    con = duckdb.connect(str(DB_PATH))
    init_db(con)

    # Exclude the directory the DB lives in (not the whole volume) so the rest
    # of that volume still gets indexed. This covers files.db, its WAL, and the
    # working copy (files.db.scan) the web UI writes, since all live alongside
    # DB_PATH. During a UI run DB_PATH is the .scan copy, whose parent is the
    # same dir, so the exclusion holds either way.
    db_dir = str(DB_PATH.resolve().parent)
    skip_dirs = {db_dir}
    print(f"  Skipping DB directory: {db_dir}\n")

    # -------------------------------------------------------------------------
    # HASH-ONLY MODE
    # -------------------------------------------------------------------------
    if hash_only:
        rows = con.execute(
            "SELECT id, path FROM files WHERE md5 IS NULL ORDER BY size_bytes"
        ).fetchall()
        print(f"  Hash-only mode: {len(rows):,} files need hashing\n")
        errors = 0
        pbar = tqdm(rows, unit="file", desc="Hashing")
        readout = make_readout(pbar)
        for row_id, path_str in pbar:
            t0 = time.monotonic()
            h = md5_file(Path(path_str))
            if h:
                con.execute("UPDATE files SET md5 = ? WHERE id = ?", [h, row_id])
            else:
                errors += 1
            readout(path_str, time.monotonic() - t0)
        con.commit()
        print(f"\n  Done. Errors: {errors}")
        con.close()
        return

    # -------------------------------------------------------------------------
    # FULL CRAWL
    # -------------------------------------------------------------------------
    started_at = now_utc()
    files_found = 0
    files_hashed = 0
    errors = 0
    batch = []

    # Resume support -- load all already-indexed paths into memory once, so the
    # per-file skip check is an O(1) set lookup instead of a SELECT round-trip
    # to DuckDB for every file the walk touches. The UNIQUE path column plus
    # INSERT OR IGNORE remains the real dedup backstop; this just avoids the
    # expensive MIME/MD5/EXIF work for files already in the DB.
    indexed_paths = {
        row[0] for row in con.execute("SELECT path FROM files").fetchall()
    }
    print(f"  Already indexed: {len(indexed_paths):,} paths (will skip)\n")

    INSERT_SQL = """
        INSERT OR IGNORE INTO files (
            path, volume, filename, extension, size_bytes, md5,
            mime_type, is_symlink, is_hidden, inode, hard_link_count,
            created_at, modified_at, accessed_at, indexed_at,
            exif_camera_make, exif_camera_model, exif_shoot_date,
            exif_gps_lat, exif_gps_lon, exif_image_width, exif_image_height,
            exif_duration_secs, exif_video_codec, exif_audio_codec,
            exif_focal_length, exif_aperture, exif_iso, exif_raw
        ) VALUES (
            ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?, ?
        )
    """

    def log_error(path_str, error_type, message):
        try:
            con.execute(
                "INSERT INTO errors (path, error_type, message, occurred_at) VALUES (?, ?, ?, ?)",
                [path_str, error_type, message, now_utc()]
            )
        except Exception:
            pass  # Never let error logging crash the crawl
        tqdm.write(f"[{error_type}] {path_str}: {message}", file=sys.stderr)

    def flush_batch():
        if not batch:
            return
        try:
            con.executemany(INSERT_SQL, batch)
        except (Exception, KeyboardInterrupt) as e:
            if isinstance(e, KeyboardInterrupt):
                raise
            # One poisoned row (e.g. a filename DuckDB can't bind) would abort
            # the whole executemany. Fall back to row-by-row so a single bad row
            # costs one row, not the batch, and gets logged instead of crashing.
            for row in batch:
                try:
                    con.execute(INSERT_SQL, list(row))
                except (Exception, KeyboardInterrupt) as re:
                    if isinstance(re, KeyboardInterrupt):
                        raise
                    log_error(row[0], type(re).__name__, str(re))
        con.commit()
        batch.clear()

    et_ctx = (exiftool.ExifToolHelper(executable=EXIFTOOL_EXECUTABLE)
              if EXIFTOOL_AVAILABLE else None)

    pbar = tqdm(unit="file", desc="Indexing", smoothing=0.05, mininterval=0.2)
    readout = make_readout(pbar)
    current_volume = None

    try:
        # Drop any CRAWL_ROOT that is itself the DB directory (or under it).
        CRAWL_ROOTS_EFFECTIVE = [
            r for r in CRAWL_ROOTS if not should_skip(r, skip_dirs)
        ]
        tqdm.write(f"  Effective crawl roots: {[str(r) for r in CRAWL_ROOTS_EFFECTIVE]}\n", file=sys.stderr)

        for root in CRAWL_ROOTS_EFFECTIVE:
            if not root.exists():
                tqdm.write(f"  [skip] Root does not exist: {root}", file=sys.stderr)
                continue

            tqdm.write(f"  Crawling: {root}", file=sys.stderr)

            for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
                dir_path = Path(dirpath)

                if should_skip(dir_path, skip_dirs):
                    dirnames.clear()
                    continue

                # Prune excluded subdirectories before os.walk descends into them
                dirnames[:] = [
                    d for d in dirnames
                    if not should_skip(dir_path / d, skip_dirs)
                ]

                dir_volume = get_volume(dir_path)
                if dir_volume != current_volume:
                    current_volume = dir_volume
                    pbar.set_description_str(f"Indexing [{dir_volume}]")

                for filename in filenames:
                    file_path = dir_path / filename
                    pbar.update(1)

                    try:
                        is_symlink = file_path.is_symlink()
                        stat = file_path.lstat()
                        size = stat.st_size
                        path_str = str(file_path)

                        # Resume support -- skip files already in the database.
                        # Heartbeat the readout here too, so fast skip-heavy
                        # stretches still show roughly where the walk is.
                        if path_str in indexed_paths:
                            readout(path_str)
                            continue

                        # A filename with bytes DuckDB can't bind as TEXT (lone
                        # surrogates from non-UTF-8 names on HFS+/SMB/archives)
                        # would crash the insert with a non-OSError that escapes
                        # the handlers below. Log and skip it instead.
                        try:
                            path_str.encode("utf-8")
                        except UnicodeEncodeError:
                            errors += 1
                            log_error(path_str, "UnicodeEncodeError",
                                      "filename is not valid UTF-8; cannot index")
                            continue

                        t0 = time.monotonic()
                        ext = file_path.suffix.lower()
                        volume = dir_volume
                        is_hidden = filename.startswith(".")

                        created_at  = ts_to_dt(stat.st_birthtime if hasattr(stat, "st_birthtime") else stat.st_ctime)
                        modified_at = ts_to_dt(stat.st_mtime)
                        accessed_at = ts_to_dt(stat.st_atime)

                        mime = None
                        if not is_symlink:
                            mime = get_mime(file_path)

                        md5 = None
                        if do_hash and not is_symlink and size > 0:
                            md5 = md5_file(file_path)
                            if md5:
                                files_hashed += 1

                        exif = {}
                        if (
                            EXIFTOOL_AVAILABLE
                            and et_ctx
                            and not is_symlink
                            and ext in EXIF_EXTENSIONS
                            and size > 0
                        ):
                            exif = get_exif(et_ctx, path_str)

                        batch.append((
                            path_str,
                            volume,
                            filename,
                            ext,
                            size,
                            md5,
                            mime,
                            is_symlink,
                            is_hidden,
                            stat.st_ino,
                            stat.st_nlink,
                            created_at,
                            modified_at,
                            accessed_at,
                            now_utc(),
                            exif.get("exif_camera_make"),
                            exif.get("exif_camera_model"),
                            exif.get("exif_shoot_date"),
                            exif.get("exif_gps_lat"),
                            exif.get("exif_gps_lon"),
                            exif.get("exif_image_width"),
                            exif.get("exif_image_height"),
                            exif.get("exif_duration_secs"),
                            exif.get("exif_video_codec"),
                            exif.get("exif_audio_codec"),
                            exif.get("exif_focal_length"),
                            exif.get("exif_aperture"),
                            exif.get("exif_iso"),
                            exif.get("exif_raw"),
                        ))
                        # Keep the in-memory resume set in step with the batch,
                        # so a path seen twice in one walk isn't reprocessed.
                        indexed_paths.add(path_str)

                        files_found += 1
                        readout(path_str, time.monotonic() - t0)

                        if len(batch) >= COMMIT_BATCH_SIZE:
                            flush_batch()

                    except PermissionError as e:
                        errors += 1
                        log_error(str(file_path), "PermissionError", str(e))
                    except OSError as e:
                        errors += 1
                        log_error(str(file_path), "OSError", str(e))

        flush_batch()

    except KeyboardInterrupt:
        tqdm.write("\n\n  Interrupted -- flushing current batch to DB...", file=sys.stderr)
        flush_batch()
        tqdm.write("  Progress saved. Run again to resume.", file=sys.stderr)

    finally:
        pbar.close()

        if et_ctx:
            try:
                et_ctx.__exit__(None, None, None)
            except Exception:
                pass

        finished_at = now_utc()
        elapsed = finished_at - started_at
        hours, remainder = divmod(int(elapsed.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)

        try:
            con.execute("""
                INSERT INTO crawl_log
                    (started_at, finished_at, host, files_found, files_hashed, errors, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [
                started_at, finished_at,
                socket.gethostname(),
                files_found, files_hashed, errors,
                f"do_hash={do_hash}",
            ])
            con.commit()
        except Exception as e:
            print(f"\n  Warning: could not write crawl_log entry: {e}")

        con.close()

        print(f"\n{'='*60}")
        print(f"  Crawl complete")
        print(f"  Elapsed:      {hours}h {minutes}m {seconds}s")
        print(f"  Files found:  {files_found:,}")
        print(f"  Files hashed: {files_hashed:,}")
        print(f"  Errors:       {errors:,}")
        print(f"  Database:     {DB_PATH}")
        print(f"{'='*60}\n")


# =============================================================================
# SYNC HELPERS (shared by --prune and --reindex-changed)
# =============================================================================

def volume_root(path: str) -> str:
    """Mount root for a path: /Volumes/NAME/rest -> /Volumes/NAME, else /."""
    if path.startswith("/Volumes/"):
        parts = path.split("/", 3)
        if len(parts) >= 3 and parts[2]:
            return "/Volumes/" + parts[2]
    return "/"


def make_mount_checker():
    """Return an is_mounted(root) predicate that caches os.path.ismount results."""
    cache = {}

    def is_mounted(root: str) -> bool:
        if root not in cache:
            cache[root] = os.path.ismount(root)
        return cache[root]

    return is_mounted


# =============================================================================
# PRUNE MODE
# =============================================================================

def prune():
    """Remove DB rows whose files no longer exist on disk.

    Safety guard: a row is only pruned if the volume its path lives on is
    currently mounted. Rows on unmounted/ejected volumes are left untouched,
    so running --prune with an external drive disconnected can never wipe that
    drive's index. Symlinks are checked with lexists, so a broken symlink (a
    real on-disk entry) is kept, matching how the crawler indexes it via lstat.
    """
    print(f"\n{'='*60}")
    print(f"  Prune mode — removing rows for deleted files")
    print(f"  Database: {DB_PATH}")
    print(f"{'='*60}\n")

    con = duckdb.connect(str(DB_PATH))

    rows = con.execute("SELECT id, path FROM files").fetchall()
    print(f"  {len(rows):,} indexed paths to verify\n")

    is_mounted = make_mount_checker()

    missing_ids = []
    skipped_offline = 0
    offline_roots = set()

    for row_id, path_str in tqdm(rows, unit="file", desc="Verifying"):
        root = volume_root(path_str)
        if not is_mounted(root):
            skipped_offline += 1
            offline_roots.add(root)
            continue
        if not os.path.lexists(path_str):
            missing_ids.append(row_id)

    deleted = 0
    BATCH = 1000
    for i in range(0, len(missing_ids), BATCH):
        chunk = missing_ids[i:i + BATCH]
        placeholders = ",".join(["?"] * len(chunk))
        con.execute(f"DELETE FROM files WHERE id IN ({placeholders})", chunk)
        deleted += len(chunk)
    con.commit()
    con.close()

    print(f"\n{'='*60}")
    print(f"  Prune complete")
    print(f"  Verified:          {len(rows):,}")
    print(f"  Deleted (missing): {deleted:,}")
    if skipped_offline:
        print(f"  Skipped (offline): {skipped_offline:,} on volumes not mounted:")
        for r in sorted(offline_roots):
            print(f"                       {r}")
    print(f"  Database:          {DB_PATH}")
    print(f"{'='*60}\n")


# =============================================================================
# PRUNE-EXCLUDED MODE
# =============================================================================

def prune_excluded():
    """Remove DB rows whose path is now covered by the exclude list.

    This lets exclude-list edits apply retroactively. It uses the same
    component-aware path-prefix rule as should_skip(), so excluding /x/FOO
    removes /x/FOO/bar but not /x/FOO_BAR/bar. It does not touch the filesystem,
    so it is safe for rows on disconnected volumes.
    """
    print(f"\n{'='*60}")
    print("  Prune-excluded mode — removing rows now under the exclude list")
    print(f"  Database: {DB_PATH}")
    print(f"{'='*60}\n")

    con = duckdb.connect(str(DB_PATH))

    before = con.execute("SELECT count(*) FROM files").fetchone()[0]
    print(f"  {before:,} rows in index")

    user = sorted(load_user_excludes())
    if user:
        print("\n  Rows matched per user exclude entry:")
        for ex in user:
            n = con.execute(
                "SELECT count(*) FROM files WHERE path = ? OR starts_with(path, ?)",
                [ex, ex + "/"],
            ).fetchone()[0]
            print(f"    {n:>12,}  {ex}")

    conds, params = [], []
    for ex in sorted(EXCLUDE_PATHS):
        conds.append("(path = ? OR starts_with(path, ?))")
        params.extend([ex, ex + "/"])
    if not conds:
        print("\n  Exclude list is empty — nothing to prune.\n")
        con.close()
        return

    con.execute(f"DELETE FROM files WHERE {' OR '.join(conds)}", params)
    con.commit()
    after = con.execute("SELECT count(*) FROM files").fetchone()[0]
    con.close()

    print(f"\n{'='*60}")
    print("  Prune-excluded complete")
    print(f"  Before:   {before:,}")
    print(f"  Deleted:  {before - after:,}")
    print(f"  After:    {after:,}")
    print("  Compact the DB to reclaim the freed space.")
    print(f"  Database: {DB_PATH}")
    print(f"{'='*60}\n")


# =============================================================================
# REINDEX-CHANGED MODE
# =============================================================================

def reindex_changed(do_hash: bool = True):
    """Refresh rows whose on-disk file changed since it was indexed.

    A file is considered changed if its size differs, or its mtime differs by
    >=1s (second granularity avoids false positives from sub-second float
    jitter in stat timestamps round-tripping through DuckDB's TIMESTAMP type).
    Changed rows get fully re-extracted metadata (size, md5, mime, stat fields,
    EXIF). Same mount-guard as --prune: paths on unmounted volumes are skipped.
    Missing files are left for --prune to remove, not deleted here.
    """
    print(f"\n{'='*60}")
    print(f"  Reindex-changed mode — refreshing modified files")
    print(f"  Database: {DB_PATH}")
    print(f"  Hashing:  {'yes' if do_hash else 'no'}")
    print(f"{'='*60}\n")

    con = duckdb.connect(str(DB_PATH))
    rows = con.execute(
        "SELECT id, path, size_bytes, modified_at FROM files"
    ).fetchall()
    print(f"  {len(rows):,} indexed paths to check\n")

    is_mounted = make_mount_checker()

    UPDATE_SQL = """
        UPDATE files SET
            size_bytes = ?, md5 = ?, mime_type = ?, is_symlink = ?,
            inode = ?, hard_link_count = ?,
            created_at = ?, modified_at = ?, accessed_at = ?, indexed_at = ?,
            exif_camera_make = ?, exif_camera_model = ?, exif_shoot_date = ?,
            exif_gps_lat = ?, exif_gps_lon = ?, exif_image_width = ?,
            exif_image_height = ?, exif_duration_secs = ?, exif_video_codec = ?,
            exif_audio_codec = ?, exif_focal_length = ?, exif_aperture = ?,
            exif_iso = ?, exif_raw = ?
        WHERE id = ?
    """

    checked = changed = missing = skipped_offline = errors = 0
    offline_roots = set()
    et_ctx = (exiftool.ExifToolHelper(executable=EXIFTOOL_EXECUTABLE)
              if EXIFTOOL_AVAILABLE else None)

    pbar = tqdm(rows, unit="file", desc="Checking")
    readout = make_readout(pbar)

    try:
        for row_id, path_str, old_size, old_mtime in pbar:
            readout(path_str)  # heartbeat for the fast skip/unchanged stretches
            if not is_mounted(volume_root(path_str)):
                skipped_offline += 1
                offline_roots.add(volume_root(path_str))
                continue

            file_path = Path(path_str)
            try:
                if not file_path.is_symlink() and not file_path.exists():
                    missing += 1
                    continue
                stat = file_path.lstat()
            except (PermissionError, OSError):
                missing += 1
                continue

            checked += 1

            # Has it changed? Compare size and mtime (second granularity).
            old_epoch = (
                old_mtime.replace(tzinfo=timezone.utc).timestamp()
                if old_mtime is not None else None
            )
            mtime_changed = old_epoch is None or abs(stat.st_mtime - old_epoch) >= 1
            if stat.st_size == old_size and not mtime_changed:
                continue

            # Re-extract full metadata for the changed file.
            t0 = time.monotonic()
            try:
                is_symlink = file_path.is_symlink()
                size = stat.st_size
                ext = file_path.suffix.lower()

                mime = get_mime(file_path) if not is_symlink else None
                md5 = None
                if do_hash and not is_symlink and size > 0:
                    md5 = md5_file(file_path)

                exif = {}
                if (
                    EXIFTOOL_AVAILABLE and et_ctx and not is_symlink
                    and ext in EXIF_EXTENSIONS and size > 0
                ):
                    exif = get_exif(et_ctx, path_str)

                con.execute(UPDATE_SQL, [
                    size, md5, mime, is_symlink,
                    stat.st_ino, stat.st_nlink,
                    ts_to_dt(stat.st_birthtime if hasattr(stat, "st_birthtime") else stat.st_ctime),
                    ts_to_dt(stat.st_mtime),
                    ts_to_dt(stat.st_atime),
                    now_utc(),
                    exif.get("exif_camera_make"), exif.get("exif_camera_model"),
                    exif.get("exif_shoot_date"), exif.get("exif_gps_lat"),
                    exif.get("exif_gps_lon"), exif.get("exif_image_width"),
                    exif.get("exif_image_height"), exif.get("exif_duration_secs"),
                    exif.get("exif_video_codec"), exif.get("exif_audio_codec"),
                    exif.get("exif_focal_length"), exif.get("exif_aperture"),
                    exif.get("exif_iso"), exif.get("exif_raw"),
                    row_id,
                ])
                changed += 1
                readout(path_str, time.monotonic() - t0)
                if changed % COMMIT_BATCH_SIZE == 0:
                    con.commit()
            except (PermissionError, OSError):
                errors += 1

        con.commit()
    except KeyboardInterrupt:
        tqdm.write("\n\n  Interrupted -- committing updates so far...", file=sys.stderr)
        con.commit()
    finally:
        if et_ctx:
            try:
                et_ctx.__exit__(None, None, None)
            except Exception:
                pass
        con.close()

    print(f"\n{'='*60}")
    print(f"  Reindex-changed complete")
    print(f"  Verified:          {checked:,}")
    print(f"  Refreshed:         {changed:,}")
    print(f"  Missing (use --prune): {missing:,}")
    if skipped_offline:
        print(f"  Skipped (offline): {skipped_offline:,} on volumes not mounted:")
        for r in sorted(offline_roots):
            print(f"                       {r}")
    print(f"  Database:          {DB_PATH}")
    print(f"{'='*60}\n")


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="File system crawler")
    parser.add_argument("--no-hash",   action="store_true", help="Skip MD5 hashing (fast metadata-only pass)")
    parser.add_argument("--hash-only", action="store_true", help="Only hash files already in DB that have no hash")
    parser.add_argument("--prune",     action="store_true", help="Remove DB rows for files that no longer exist on disk (skips offline volumes)")
    parser.add_argument("--prune-excluded", action="store_true", help="Remove DB rows now covered by the exclude list (retroactive exclude; compact after)")
    parser.add_argument("--reindex-changed", action="store_true", help="Refresh rows whose on-disk file changed (size/mtime); skips offline volumes")
    parser.add_argument("--db", help="Operate on this DB file instead of the default (used by the web UI to run against a disposable copy)")
    args = parser.parse_args()

    if args.db:
        DB_PATH = Path(args.db)

    if args.prune:
        prune()
    elif args.prune_excluded:
        prune_excluded()
    elif args.reindex_changed:
        reindex_changed(do_hash=not args.no_hash)
    elif args.hash_only:
        crawl(do_hash=False, hash_only=True)
    else:
        crawl(do_hash=not args.no_hash)
