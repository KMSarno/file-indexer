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
import re
import socket
import shutil
from datetime import datetime, timezone
from pathlib import Path

import duckdb

# Packaged builds export KENDEX_LIBMAGIC pointing at the bundled libmagic.dylib.
# python-magic's loader probes find_library('magic') first, then hardcoded paths
# like /opt/homebrew/lib — so on a machine with a different *system* libmagic it
# would load that one (whose magic.mgc format version may not match our bundled
# database), and a hardened-runtime app can't pass DYLD_* to redirect it. Pin
# find_library to the bundled lib so the dylib and our bundled magic.mgc always
# match. No-op in dev (env unset): python-magic resolves libmagic normally.
_bundled_libmagic = os.environ.get("KENDEX_LIBMAGIC")
if _bundled_libmagic and os.path.exists(_bundled_libmagic):
    import ctypes.util as _ctypes_util
    _orig_find_library = _ctypes_util.find_library
    _ctypes_util.find_library = lambda name: (
        _bundled_libmagic if name == "magic" else _orig_find_library(name)
    )

try:
    import magic
except Exception:  # native libmagic missing — degrade gracefully (MIME → None)
    magic = None
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
#
# It lives next to the DATABASE (a persistent, writable location — the desktop
# app's Application Support dir), NOT next to this script: the script ships
# inside the .app bundle, which is *replaced on every update* (silently wiping
# the user's exclude list) and is code-signed/notarized (writing into it breaks
# the signature). An exclude list saved by an older version next to the script
# (`_LEGACY_EXCLUDE_CONFIG`) is still read as a fallback so upgrades don't lose it.
EXCLUDE_CONFIG = DB_PATH.parent / "exclude_paths.json"
_LEGACY_EXCLUDE_CONFIG = Path(__file__).resolve().parent / "exclude_paths.json"


# --- Exclude matching --------------------------------------------------------
# An exclude entry is one of two kinds:
#   * a plain absolute path -> component-aware PREFIX match ("/x/FOO" excludes
#     "/x/FOO" and "/x/FOO/..." but never the sibling "/x/FOO_BAR"); children are
#     covered automatically.
#   * a glob pattern -> matched against the WHOLE path. "*" means any run of
#     characters (including "/"), "?" means any single character. An entry
#     containing either metacharacter is treated as a glob, e.g.
#     "*/Library/Application Support/*" excludes that folder's contents under
#     every user. No other characters are special ("[" is literal), so the
#     crawl-time matcher (regex) and the prune matcher (SQL LIKE) stay identical.
# Because a glob matches the whole path, add a trailing "*" (or "/*") to catch a
# folder's contents -- "*/Foo" alone matches only the folder path itself.

def _has_glob(pattern: str) -> bool:
    return "*" in pattern or "?" in pattern


def is_valid_exclude(entry) -> bool:
    """A usable exclude entry: an absolute path, or a glob with at least one
    literal anchor. Rejects empty strings and match-everything patterns like
    "*" or "/*" -- via --prune-excluded those would delete the whole index."""
    if not isinstance(entry, str):
        return False
    entry = entry.strip()
    if not entry:
        return False
    if not (entry.startswith("/") or _has_glob(entry)):
        return False
    return any(c not in "*?/" for c in entry)  # require a literal anchor


def _glob_to_regex(pattern: str):
    """Compile a "*"/"?" glob to an anchored, full-path regex."""
    parts = []
    for ch in pattern:
        if ch == "*":
            parts.append(".*")
        elif ch == "?":
            parts.append(".")
        else:
            parts.append(re.escape(ch))
    return re.compile("".join(parts) + r"\Z", re.DOTALL)


def _glob_to_like(pattern: str) -> str:
    """Translate a "*"/"?" glob to a DuckDB LIKE pattern (used with ESCAPE '\\')."""
    out = []
    for ch in pattern:
        if ch == "*":
            out.append("%")
        elif ch == "?":
            out.append("_")
        elif ch in "%_\\":
            out.append("\\" + ch)
        else:
            out.append(ch)
    return "".join(out)


def load_user_excludes() -> set:
    """Read the user exclude list. Missing/garbage file -> no user excludes.
    Only valid entries (absolute paths or anchored globs) are accepted."""
    # Prefer the persistent location; fall back to a list an older version left
    # next to the script so an upgrade carries the user's excludes forward.
    path = EXCLUDE_CONFIG
    if not path.exists() and _LEGACY_EXCLUDE_CONFIG.exists():
        path = _LEGACY_EXCLUDE_CONFIG
    try:
        with open(path) as f:
            data = json.load(f)
    except (FileNotFoundError, ValueError, OSError):
        return set()
    if not isinstance(data, list):
        return set()
    return {p.strip() for p in data if is_valid_exclude(p)}


# The effective exclude set: locked defaults plus whatever the UI saved.
EXCLUDE_PATHS = EXCLUDE_DEFAULTS | load_user_excludes()


def _split_excludes(paths):
    """Partition the exclude set into literal prefixes and precompiled globs
    so should_skip does no per-call regex compilation."""
    literals, globs = set(), []
    for ex in paths:
        if _has_glob(ex):
            globs.append(_glob_to_regex(ex))
        else:
            literals.add(ex)
    return literals, globs


_EXCLUDE_LITERALS, _EXCLUDE_GLOBS = _split_excludes(EXCLUDE_PATHS)

# File extensions that get full EXIF extraction (slower but rich metadata)
EXIF_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".tif", ".tiff", ".heic", ".heif",
    ".cr2", ".cr3", ".nef", ".arw", ".raf", ".dng", ".rw2",
    ".mp4", ".mov", ".mxf", ".braw", ".avi", ".mkv", ".m4v",
    ".mp3", ".wav", ".aiff", ".flac", ".m4a",
}

# --- Listed-types tag --------------------------------------------------------
# An optional "these are the file types I care about" list, the type-level twin
# of the path-level exclude list. It does NOT gate indexing: the crawl still
# walks and records every non-excluded file. Instead each row is tagged with the
# `is_listed_type` flag (true when its extension is in the set below), and the
# query UI's "Listed types only" switch filters on that flag. Net effect: a
# complete index, with a one-click view that hides the types you don't care about.
#
# Unlike the exclude defaults (which are locked for safety -- see EXCLUDE_DEFAULTS),
# every INCLUDE default is freely removable from the UI: omitting a type can't
# corrupt anything, it just means "don't tag that type as listed". The config
# stores the two small deltas from the defaults, so a future release growing
# INCLUDE_DEFAULTS is picked up automatically instead of freezing the user to
# today's set.
INCLUDE_DEFAULTS = frozenset({
    # documents
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".pages", ".numbers", ".key",          # Apple iWork (Keynote is ".key")
    ".txt", ".rtf", ".md", ".csv",
    # graphics
    ".psd", ".ai", ".jpg", ".jpeg", ".png", ".tif", ".tiff",
    ".heic", ".gif", ".webp", ".svg",
    # audio / video
    ".mp3", ".m4a", ".wav", ".aiff", ".flac",
    ".mp4", ".mov", ".m4v", ".avi", ".mkv",
    # archives / disk images
    ".zip", ".dmg",
})

# Lives next to the DATABASE, same persistent location and legacy fallback as the
# exclude list (see EXCLUDE_CONFIG for why-not-next-to-the-script).
INCLUDE_CONFIG = DB_PATH.parent / "include_config.json"
_LEGACY_INCLUDE_CONFIG = Path(__file__).resolve().parent / "include_config.json"


def normalize_extension(entry):
    """Normalize a user-typed extension to the stored '.xyz' lowercase form, or
    None if it isn't a usable single-suffix extension. Accepts 'pdf', '.PDF',
    ' .pdf '; rejects empties, compound suffixes ('.tar.gz'), and anything with
    a slash/space/glob char."""
    if not isinstance(entry, str):
        return None
    s = entry.strip().lower()
    if not s:
        return None
    if not s.startswith("."):
        s = "." + s
    body = s[1:]
    if not body or not body.isalnum():   # one segment, alphanumerics only
        return None
    return s


def is_valid_extension(entry) -> bool:
    return normalize_extension(entry) is not None


def load_include_config() -> dict:
    """Read the include config's two delta sets. Missing/garbage -> empty deltas.
    Only the *contents* are read here; whether the filter is active is decided by
    effective_includes() (which also accounts for the file's existence)."""
    path = INCLUDE_CONFIG
    if not path.exists() and _LEGACY_INCLUDE_CONFIG.exists():
        path = _LEGACY_INCLUDE_CONFIG
    try:
        with open(path) as f:
            data = json.load(f)
    except (FileNotFoundError, ValueError, OSError):
        return {"disabled": set(), "added": set()}
    if not isinstance(data, dict):
        return {"disabled": set(), "added": set()}

    def _clean(key):
        vals = data.get(key, [])
        if not isinstance(vals, list):
            return set()
        return {e for e in (normalize_extension(v) for v in vals) if e}

    return {"disabled": _clean("disabled"), "added": _clean("added")}


def effective_includes():
    """The active listed-type set as a frozenset, or None when the list is OFF
    (every type counts as listed). OFF when no config file exists -- so the
    default, back-compatible behavior is "treat all types as listed" until the
    user saves a list -- or when the effective set is empty (the safe failure
    mode: never silently tag zero files, which would make the "Listed types
    only" switch hide everything)."""
    if not INCLUDE_CONFIG.exists() and not _LEGACY_INCLUDE_CONFIG.exists():
        return None
    cfg = load_include_config()
    eff = (set(INCLUDE_DEFAULTS) - cfg["disabled"]) | cfg["added"]
    return frozenset(eff) if eff else None


# Active listed-type set, computed once at import (the crawl subprocess re-reads
# it on its own startup; query_app.py reads the deltas fresh per request).
INCLUDE_EXTENSIONS = effective_includes()


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
        is_dataless         BOOLEAN,
        is_listed_type      BOOLEAN,
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

# macOS marks an iCloud/cloud "placeholder" file (metadata present, bytes not
# downloaded) with the SF_DATALESS super-user flag, visible from stat() without
# triggering a download. Reading such a file's *content* (MIME sniff, hash,
# EXIF) forces the OS to fetch it from iCloud first — ~1s per file. We detect
# the flag and index dataless files from metadata alone instead.
SF_DATALESS = 0x40000000

# Columns added after the original schema shipped; ADD-COLUMN-IF-NOT-EXISTS
# brings an existing files.db up to date without a rebuild.
MIGRATIONS = [
    "ALTER TABLE files ADD COLUMN IF NOT EXISTS is_dataless BOOLEAN",
    "ALTER TABLE files ADD COLUMN IF NOT EXISTS is_listed_type BOOLEAN",
]


# =============================================================================
# HELPERS
# =============================================================================

def init_db(con):
    """Execute each schema statement individually, then apply migrations."""
    for stmt in SCHEMA_STATEMENTS:
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)
    for stmt in MIGRATIONS:
        try:
            con.execute(stmt)
        except Exception:
            pass  # already applied, or older DuckDB — non-fatal


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
    """Return True if this path should be excluded from crawling. The
    self-exclusion dirs and plain-path excludes match on a full path component
    (exact, or a '/'-delimited prefix) so e.g. '/x/KMSDB_PROJ' never shadows a
    sibling '/x/KMSDB_PROJ_BACKUP'; glob excludes match the whole path (see the
    exclude-matching helpers above)."""
    path_str = str(path)
    for d in skip_dirs:
        if path_str == d or path_str.startswith(d + "/"):
            return True
    for ex in _EXCLUDE_LITERALS:
        if path_str == ex or path_str.startswith(ex + "/"):
            return True
    for rx in _EXCLUDE_GLOBS:
        if rx.match(path_str):
            return True
    return False


# =============================================================================
# MAIN CRAWL
# =============================================================================

def crawl(do_hash: bool = True, hash_only: bool = False, dupes_only: bool = False,
          stat_only: bool = False, include_dataless: bool = False,
          roots: list | None = None):
    mode_label = ("metadata only (stat-only)" if stat_only
                  else "yes" if do_hash else "no hashing")
    print(f"\n{'='*60}")
    print(f"  File System Crawler")
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Database: {DB_PATH}")
    print(f"  Content reads: {mode_label}")
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

    # Also exclude our own app bundle when running packaged (Electron Kendex.app).
    # crawler.py then lives at .../Kendex.app/Contents/Resources/backend/crawler.py,
    # and the bundled runtime ships a libmagic magic.mgc that floods the log if
    # indexed -- and an app should never index its own guts. Detect the nearest
    # ancestor whose name ends in '.app'; absent (dev / from-source run), nothing
    # extra is excluded.
    app_bundle = next((p for p in Path(__file__).resolve().parents
                       if p.name.endswith(".app")), None)
    if app_bundle is not None:
        skip_dirs.add(str(app_bundle))
        print(f"  Skipping app bundle: {app_bundle}\n")

    # Report the listed-types tag (None => every type counts as listed). Note
    # this only tags rows; every non-excluded file is indexed regardless.
    if INCLUDE_EXTENSIONS is not None:
        print(f"  Tagging {len(INCLUDE_EXTENSIONS)} file types as listed: "
              f"{' '.join(sorted(INCLUDE_EXTENSIONS))}\n")
    else:
        print("  Tagging all file types as listed (no list configured)\n")

    # -------------------------------------------------------------------------
    # HASH-ONLY MODE
    # -------------------------------------------------------------------------
    if hash_only:
        if dupes_only:
            # Only files whose size collides with another file can be byte
            # duplicates, so hashing a unique-size file just reads its bytes to
            # confirm it has no twin -- wasted I/O. Hash only the size-collision
            # groups (symlinks and zero-byte files excluded, matching the full
            # crawl). Same size is necessary but NOT sufficient for a duplicate,
            # so these still get a real MD5 to confirm. Unique-size files keep
            # md5 = NULL; the Duplicate-files report filters on md5 IS NOT NULL,
            # so it stays correct.
            # Dataless iCloud files would each download when hashed; skip them
            # by default so a dedup pass never silently pulls gigabytes from the
            # cloud. --include-dataless opts in.
            dataless_clause = ("" if include_dataless
                               else "AND coalesce(is_dataless, false) = false ")
            rows = con.execute(
                "SELECT id, path FROM files "
                "WHERE md5 IS NULL AND coalesce(is_symlink, false) = false "
                f"  {dataless_clause}"
                "  AND size_bytes > 0 "
                "  AND size_bytes IN ("
                "    SELECT size_bytes FROM files "
                "    WHERE size_bytes > 0 AND coalesce(is_symlink, false) = false "
                "    GROUP BY size_bytes HAVING count(*) > 1"
                "  ) "
                "ORDER BY size_bytes"
            ).fetchall()
            print(f"  Hash-dupes mode: {len(rows):,} files in size-collision "
                  f"groups need hashing")
            print("  (files with a unique size can't be duplicates -- skipped"
                  + ("" if include_dataless else "; dataless iCloud files skipped")
                  + ")\n")
        else:
            dataless_clause = ("" if include_dataless
                               else "AND coalesce(is_dataless, false) = false ")
            rows = con.execute(
                "SELECT id, path FROM files WHERE md5 IS NULL "
                f"{dataless_clause}ORDER BY size_bytes"
            ).fetchall()
            print(f"  Hash-only mode: {len(rows):,} files need hashing\n")
        errors = 0
        hashed = set()                      # md5s computed this run, for the report
        pbar = tqdm(rows, unit="file", desc="Hashing")
        readout = make_readout(pbar)
        for row_id, path_str in pbar:
            t0 = time.monotonic()
            h = md5_file(Path(path_str))
            if h:
                con.execute("UPDATE files SET md5 = ? WHERE id = ?", [h, row_id])
                hashed.add(h)
            else:
                errors += 1
            readout(path_str, time.monotonic() - t0)
        con.commit()

        # Report how many duplicate sets this run logged -- md5 groups (>1 copy)
        # that include a file hashed just now. Run-scoped, so re-running doesn't
        # re-announce old duplicates. Counted via a temp table since the set of
        # new md5s can be large.
        dup_sets = 0
        if hashed:
            con.execute("CREATE OR REPLACE TEMP TABLE _new_md5 (md5 TEXT)")
            con.executemany("INSERT INTO _new_md5 VALUES (?)", [[h] for h in hashed])
            dup_sets = con.execute(
                "SELECT count(*) FROM ("
                "  SELECT md5 FROM files WHERE md5 IN (SELECT md5 FROM _new_md5) "
                "  GROUP BY md5 HAVING count(*) > 1)"
            ).fetchone()[0]
        con.close()

        print(f"\n  Done. Errors: {errors}")
        print(f"\n{'='*60}")
        if dup_sets:
            print(f"  {dup_sets:,} duplicate set{'s' if dup_sets != 1 else ''} "
                  f"logged -- see the Duplicate Manager")
        else:
            print("  No duplicate sets logged")
        print(f"{'='*60}\n")
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
            mime_type, is_symlink, is_dataless, is_listed_type, is_hidden, inode, hard_link_count,
            created_at, modified_at, accessed_at, indexed_at,
            exif_camera_make, exif_camera_model, exif_shoot_date,
            exif_gps_lat, exif_gps_lon, exif_image_width, exif_image_height,
            exif_duration_secs, exif_video_codec, exif_audio_codec,
            exif_focal_length, exif_aperture, exif_iso, exif_raw
        ) VALUES (
            ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?,
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
        # A selective scan (roots given) restricts the walk to the chosen
        # volumes; the default crawl uses CRAWL_ROOTS (just "/"). Each external
        # is walked via its own /Volumes/NAME root, so when "/" is selected too
        # it must NOT descend into /Volumes (that would re-walk every external,
        # selected or not) -- prune that subtree for the boot root only.
        selective = bool(roots)
        base_roots = [Path(r) for r in roots] if selective else CRAWL_ROOTS
        # Drop any root that is itself the DB directory (or under it).
        CRAWL_ROOTS_EFFECTIVE = [
            r for r in base_roots if not should_skip(r, skip_dirs)
        ]
        tqdm.write(f"  Effective crawl roots: {[str(r) for r in CRAWL_ROOTS_EFFECTIVE]}\n", file=sys.stderr)

        for root in CRAWL_ROOTS_EFFECTIVE:
            if not root.exists():
                tqdm.write(f"  [skip] Root does not exist: {root}", file=sys.stderr)
                continue

            tqdm.write(f"  Crawling: {root}", file=sys.stderr)
            prune_volumes = selective and root == Path("/")

            for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
                dir_path = Path(dirpath)

                if should_skip(dir_path, skip_dirs):
                    dirnames.clear()
                    continue

                # Boot root in a selective scan: don't cross into other volumes.
                if prune_volumes and dir_path == Path("/Volumes"):
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

                        # A dataless iCloud placeholder downloads the moment its
                        # bytes are read. stat() (and thus the flag) is free, so
                        # we index it from metadata and never touch its content.
                        is_dataless = bool(getattr(stat, "st_flags", 0) & SF_DATALESS)
                        # Tag the row's type against the listed-types set (free,
                        # extension-only). None => no list configured, so every
                        # type counts as listed. The "Listed types only" query
                        # switch filters on this flag; the file is indexed either way.
                        is_listed_type = (INCLUDE_EXTENSIONS is None
                                          or ext in INCLUDE_EXTENSIONS)
                        # stat_only: a pure metadata sweep — no content reads at
                        # all, for any file. The fast first pass.
                        read_content = not stat_only and not is_dataless

                        mime = None
                        if read_content and not is_symlink:
                            mime = get_mime(file_path)

                        md5 = None
                        if read_content and do_hash and not is_symlink and size > 0:
                            md5 = md5_file(file_path)
                            if md5:
                                files_hashed += 1

                        exif = {}
                        if (
                            read_content
                            and EXIFTOOL_AVAILABLE
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
                            is_dataless,
                            is_listed_type,
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

    This lets exclude-list edits apply retroactively, using the same matching
    rules as should_skip(): plain paths are component-aware prefixes (excluding
    /x/FOO removes /x/FOO/bar but not /x/FOO_BAR/bar) and glob entries match the
    whole path via SQL LIKE. It does not touch the filesystem, so it is safe for
    rows on disconnected volumes.
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
            if _has_glob(ex):
                n = con.execute(
                    "SELECT count(*) FROM files WHERE path LIKE ? ESCAPE '\\'",
                    [_glob_to_like(ex)],
                ).fetchone()[0]
            else:
                n = con.execute(
                    "SELECT count(*) FROM files WHERE path = ? OR starts_with(path, ?)",
                    [ex, ex + "/"],
                ).fetchone()[0]
            print(f"    {n:>12,}  {ex}")

    conds, params = [], []
    for ex in sorted(EXCLUDE_PATHS):
        if _has_glob(ex):
            conds.append("path LIKE ? ESCAPE '\\'")
            params.append(_glob_to_like(ex))
        else:
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


def reflag_types():
    """Recompute every row's is_listed_type flag from the active listed-types set.

    The retroactive counterpart to the crawl-time tagging (like --prune-excluded
    is to the exclude list): after editing the list, this re-evaluates the flag
    on rows the walk won't revisit (resume skips already-indexed paths). It is a
    single bulk UPDATE over the whole table, so it touches every row regardless
    of when it was added -- that's the point, since the walk only tags new rows.
    Non-destructive: it only flips a boolean, never deletes a row, so there is
    nothing to compact afterward. Pure SQL, no disk access and no mount guard,
    so it is safe with volumes offline. When no list is configured every row is
    tagged listed (the "Listed types only" switch is then a no-op).
    """
    print(f"\n{'='*60}")
    print("  Re-tag mode — recomputing the is_listed_type flag")
    print(f"  Database: {DB_PATH}")
    print(f"{'='*60}\n")

    con = duckdb.connect(str(DB_PATH))
    # Ensure the schema is current — on a DB that predates the is_listed_type
    # column this is the first op that needs it, and (unlike the crawl path)
    # nothing here has run the migrations yet. init_db is idempotent.
    init_db(con)
    total = con.execute("SELECT count(*) FROM files").fetchone()[0]

    if INCLUDE_EXTENSIONS is None:
        print(f"  {total:,} rows; no list configured — tagging every row as listed.\n")
        con.execute("UPDATE files SET is_listed_type = TRUE")
    else:
        exts = sorted(INCLUDE_EXTENSIONS)
        print(f"  {total:,} rows; {len(exts)} listed types: {' '.join(exts)}\n")
        placeholders = ", ".join("?" for _ in exts)
        con.execute(
            f"UPDATE files SET is_listed_type = "
            f"(coalesce(lower(extension), '') IN ({placeholders}))",
            exts,
        )
    con.commit()
    listed = con.execute(
        "SELECT count(*) FROM files WHERE coalesce(is_listed_type, false)"
    ).fetchone()[0]
    con.close()

    print(f"\n{'='*60}")
    print("  Re-tag complete")
    print(f"  Listed:     {listed:,}")
    print(f"  Not listed: {total - listed:,}")
    print(f"  Total:      {total:,}")
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
    parser.add_argument("--no-hash",   action="store_true", help="Skip MD5 hashing (still reads content for MIME/EXIF)")
    parser.add_argument("--stat-only", action="store_true", help="Pure metadata sweep: no MIME/EXIF/hash, never reads file content (won't download dataless iCloud files). The fast first pass.")
    parser.add_argument("--hash-only", action="store_true", help="Only hash files already in DB that have no hash")
    parser.add_argument("--hash-dupes", action="store_true", help="Like --hash-only but only hashes files whose size collides with another (skips unique-size files, which can't be duplicates)")
    parser.add_argument("--include-dataless", action="store_true", help="With --hash-only/--hash-dupes, also hash dataless iCloud files (downloads them); off by default")
    parser.add_argument("--prune",     action="store_true", help="Remove DB rows for files that no longer exist on disk (skips offline volumes)")
    parser.add_argument("--prune-excluded", action="store_true", help="Remove DB rows now covered by the exclude list (retroactive exclude; compact after)")
    parser.add_argument("--reflag-types", action="store_true", help="Recompute the is_listed_type flag on every row from the current listed-types list (retroactive tag; non-destructive)")
    parser.add_argument("--reindex-changed", action="store_true", help="Refresh rows whose on-disk file changed (size/mtime); skips offline volumes")
    parser.add_argument("--roots", nargs="+", metavar="PATH", help="Restrict the crawl to these root paths (selective per-volume scan, e.g. / or /Volumes/NAME); default is all of CRAWL_ROOTS")
    parser.add_argument("--db", help="Operate on this DB file instead of the default (used by the web UI to run against a disposable copy)")
    args = parser.parse_args()

    if args.db:
        DB_PATH = Path(args.db)

    if args.prune:
        prune()
    elif args.prune_excluded:
        prune_excluded()
    elif args.reflag_types:
        reflag_types()
    elif args.reindex_changed:
        reindex_changed(do_hash=not args.no_hash)
    elif args.hash_dupes:
        crawl(do_hash=False, hash_only=True, dupes_only=True,
              include_dataless=args.include_dataless)
    elif args.hash_only:
        crawl(do_hash=False, hash_only=True,
              include_dataless=args.include_dataless)
    elif args.stat_only:
        crawl(stat_only=True, roots=args.roots)
    else:
        crawl(do_hash=not args.no_hash, roots=args.roots)
