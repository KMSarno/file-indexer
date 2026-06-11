#!/usr/bin/env python3
"""
Local web UI for querying the file index (files.db).

Run:
    uv run query_app.py            # serve on http://127.0.0.1:8800
    uv run query_app.py --port 9000

Opens the DuckDB database read-only for queries, so queries can never modify the
index. The Maintenance panel shells out to the crawler to keep the DB
current; the crawler runs against a disposable copy of files.db (copy-on-write,
swapped in atomically only on success), so queries stay available during a run
and an in-progress run can be halted and discarded without touching the live DB.

Stdlib only (plus duckdb) — no web framework, no network needed.
"""

import argparse
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import sys
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import duckdb

import crawler  # single source of truth for DB_PATH, EXCLUDE_DEFAULTS, exclude config path

DB_PATH = str(crawler.DB_PATH)  # reuse the crawler's path so the two can't drift
WORK_DB = DB_PATH + ".scan"  # working copy the crawler writes to during a run
MAX_ROWS = 2000  # cap returned rows so the browser never chokes on 2.5M rows

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_PATH = os.path.join(BASE_DIR, "webapp_run.log")
# A tqdm progress line, in either shape: the percentage-bar form when the run
# has a known total ("Verifying:  40%|████  | 1112231/2762976 [..file/s]") or
# the total-less counter form a first scan emits ("Indexing [/]: 917601file
# [3:55:20, 55.64file/s, /path]"). Both must land in the status `progress`
# field — the UI's progress label and path strip read it — and be kept out of
# the scrolling log. Caught whether tqdm overwrites with \r (tty-ish) or
# prints a fresh \n line per refresh (file).
_TQDM_RE = re.compile(r"\d{1,3}%\||\d+file \[")


def get_excludes() -> dict:
    """Built-in (locked) excludes + the user-editable list, for the editor UI."""
    return {
        "defaults": sorted(crawler.EXCLUDE_DEFAULTS),
        "user": sorted(crawler.load_user_excludes()),
    }


def save_user_excludes(paths) -> dict:
    """Write the user exclude list to crawler.EXCLUDE_CONFIG. Only valid entries
    are kept (absolute paths or anchored globs, per crawler.is_valid_exclude);
    built-in defaults are never touched. Returns the saved set (or an error
    dict). This writes a plain JSON file of patterns — it does not feed the fixed
    crawler COMMANDS, so the run path stays request-input-free."""
    if not isinstance(paths, list):
        return {"error": "expected a list of paths"}
    clean = sorted({p.strip() for p in paths if crawler.is_valid_exclude(p)})
    try:
        with open(crawler.EXCLUDE_CONFIG, "w") as f:
            json.dump(clean, f, indent=2)
    except OSError as e:
        return {"error": f"could not save: {e}"}
    return {"defaults": sorted(crawler.EXCLUDE_DEFAULTS), "user": clean}


# Run subprocesses with THIS server's own interpreter (the project venv python,
# which already has every dependency) rather than "uv run". The server may be
# started by launchd, whose minimal PATH does not include uv — a bare "uv run"
# then dies with exit 127 (command not found). sys.executable needs no PATH.
PY = shlex.quote(sys.executable)


def _cmd(*flags):
    # The crawler runs against the working copy, never the live DB, so a run can
    # be discarded by deleting the copy and files.db is only touched on success.
    return PY + " crawler.py " + " ".join(flags + ("--db", shlex.quote(WORK_DB)))


# Maintenance commands — fixed strings, never built from request input.
# "compact" is the odd one out: it doesn't run the crawler on a snapshot but
# rebuilds WORK_DB itself from the live DB (attached read-only), reclaiming
# the dead space DuckDB never returns to the OS. Same swap-on-success applies.
def _cmd_live(*flags):
    # Add Files targets the LIVE db (not the disposable working copy): adding is
    # purely additive (INSERT OR IGNORE + filling NULL md5s), so it can write
    # straight through and resume after any interruption — see _run_worker.
    return PY + " crawler.py " + " ".join(flags + ("--db", shlex.quote(DB_PATH)))


COMMANDS = {
    # "add" is the one user-facing indexing action: a metadata-only sweep (fast,
    # no MD5 reads) then a size-collision-only hash pass that logs duplicates.
    # It writes directly to the live DB and is resumable (handled specially in
    # _run_worker); the rest below are copy-on-write (snapshot → swap on success).
    "add":     " && ".join([_cmd_live("--no-hash"), _cmd_live("--hash-dupes")]),
    "reindex": _cmd("--reindex-changed"),
    "scan":    _cmd(),
    "prune":   _cmd("--prune"),
    "prune_excluded": _cmd("--prune-excluded"),
    "sync":    " && ".join([_cmd("--reindex-changed"), _cmd(), _cmd("--prune")]),
    "compact": (PY + " compact_db.py "
                + shlex.quote(DB_PATH) + " " + shlex.quote(WORK_DB)),
}

# Modes that write straight to the live DB instead of a working copy (additive,
# resumable, nothing to discard).
DIRECT_MODES = {"add"}

# Read-only query connection, serialized with a lock. DuckDB cursors aren't safe
# to share across threads, so one query runs at a time — fine for a single-user
# local tool. Because maintenance runs write to WORK_DB (not the live DB),
# queries stay available against files.db during a run; _con is closed only for
# the instant of the atomic swap on success. Opened by _open_con() in main(), so
# importing this module has no side effects.
_con = None
_lock = threading.Lock()

# Maintenance run state, guarded by its own lock (never nested with _lock).
_run = {"active": False, "mode": None, "exit_code": None,
        "phase": None, "pid": None, "halt_requested": False}
_run_lock = threading.Lock()


def _open_con():
    """(Re)open the read-only query connection; leaves _con None on failure."""
    global _con
    with _lock:
        try:
            _con = duckdb.connect(DB_PATH, read_only=True)
        except Exception:
            _con = None


def _rm(path):
    try:
        os.remove(path)
    except OSError:
        pass


def _discard_work():
    _rm(WORK_DB)
    _rm(WORK_DB + ".wal")


def start_run(mode: str) -> dict:
    """Launch a maintenance command in the background. Returns immediately."""
    if mode not in COMMANDS:
        return {"error": f"unknown mode: {mode}"}
    with _run_lock:
        if _run["active"]:
            return {"error": f"a task is already running: {_run['mode']}"}
        _run.update(active=True, mode=mode, exit_code=None,
                    phase="preparing", pid=None, halt_requested=False)
    threading.Thread(target=_run_worker, args=(mode,), daemon=True).start()
    return {"ok": True, "mode": mode}


def _run_worker(mode):
    """Build WORK_DB (snapshot+crawler, or compact) → swap on success / discard on halt."""
    global _con
    log = open(LOG_PATH, "w")
    command = COMMANDS[mode]
    _discard_work()  # clear any stale leftover from a prior crash
    if mode == "sync" and not os.path.exists(DB_PATH):
        command = COMMANDS["scan"]
    log.write(f"$ {command}\n\n")

    if mode in DIRECT_MODES:
        # Additive + resumable: write straight to the live DB, no snapshot/swap,
        # nothing to discard. The read-only query connection steps aside for the
        # duration (the writer needs the database) and is reopened at the end.
        log.write("Adding files — writes directly to the index and is safe to "
                  "halt and resume; queries pause until it finishes.\n\n")
        log.flush()
        with _lock:
            try:
                if _con is not None:
                    _con.close()
            except Exception:
                pass
            _con = None
        proc = subprocess.Popen(
            command, shell=True, cwd=BASE_DIR,
            stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
        )
        with _run_lock:
            _run.update(pid=proc.pid, phase="running")
        code = proc.wait()
        log.flush()
        with _run_lock:
            halted = _run["halt_requested"]
        if halted:
            log.write("\n[stopped] Halted — files added so far are kept. "
                      "Run Add Files again to resume.\n")
        elif code != 0:
            log.write(f"\n[stopped] Exited {code} — files added so far are kept. "
                      f"Run Add Files again to resume.\n")
        else:
            log.write("\n[committed] Add Files complete.\n")
        log.close()
        _open_con()                         # reopen read-only (acquires _lock)
        with _run_lock:
            _run.update(active=False, exit_code=code, phase="done", pid=None,
                        halt_requested=False)
        return

    if mode == "compact":
        # compact_db.py builds WORK_DB itself (COPY FROM DATABASE from the
        # live DB, attached read-only) — no snapshot needed.
        log.write("Rebuilding a compacted copy of files.db "
                  "(files.db stays queryable)…\n\n")
        log.flush()
    else:
        log.write(f"Snapshotting files.db → {os.path.basename(WORK_DB)} …\n")
        log.flush()
        if os.path.exists(DB_PATH):
            try:
                shutil.copy2(DB_PATH, WORK_DB)
            except Exception as e:
                log.write(f"Snapshot failed: {e}\nAborted; files.db unchanged.\n")
                log.close()
                with _run_lock:
                    _run.update(active=False, exit_code=None, phase="error",
                                pid=None, halt_requested=False)
                return
        elif mode in ("scan", "sync"):
            log.write("No files.db exists yet; first scan will create it "
                      "in the working copy.\n")
            if mode == "sync":
                log.write("Full sync will continue as an initial scan because "
                          "there is no existing database to refresh or prune.\n")
        else:
            log.write("Snapshot failed: files.db does not exist.\n"
                      "Run 'Scan for new' first to create the database.\n")
            log.close()
            with _run_lock:
                _run.update(active=False, exit_code=None, phase="error",
                            pid=None, halt_requested=False)
            return

        # Honor a halt requested during the (long) snapshot copy.
        with _run_lock:
            halted_early = _run["halt_requested"]
        if halted_early:
            _discard_work()
            log.write("\n[discarded] Halted during snapshot; "
                      "files.db unchanged.\n")
            log.close()
            with _run_lock:
                _run.update(active=False, exit_code=None, phase="done",
                            pid=None, halt_requested=False)
            return

        log.write("Preparation done. Running crawler "
                  "(files.db stays queryable)…\n\n")
        log.flush()
    proc = subprocess.Popen(
        command, shell=True, cwd=BASE_DIR,
        stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
    )
    with _run_lock:
        _run.update(pid=proc.pid, phase="running")
    code = proc.wait()
    log.flush()

    with _run_lock:
        halted = _run["halt_requested"]

    if halted or code != 0:
        _discard_work()
        why = "Halted by user" if halted else f"Crawler exited {code}"
        log.write(f"\n[discarded] {why} — working copy removed; "
                  f"files.db unchanged.\n")
    else:
        # Success: atomically replace the live DB with the working copy.
        with _lock:
            try:
                if _con is not None:
                    _con.close()
                    _con = None
            except Exception:
                pass
            try:
                _rm(WORK_DB + ".wal")  # no-op if the crawler closed cleanly
                os.replace(WORK_DB, DB_PATH)  # atomic; old DB unlinked, not trashed
                log.write("\n[committed] working copy swapped into files.db; "
                          "previous DB removed.\n")
            except Exception as e:
                log.write(f"\n[error] swap failed: {e}; copy left at {WORK_DB}\n")
            try:
                _con = duckdb.connect(DB_PATH, read_only=True)
            except Exception:
                _con = None

    log.close()
    with _run_lock:
        _run.update(active=False, exit_code=code, phase="done", pid=None,
                    halt_requested=False)


def halt_run() -> dict:
    """Stop the running crawler and discard its working copy."""
    with _run_lock:
        if not _run["active"]:
            return {"error": "no maintenance task is running"}
        pid = _run["pid"]
        _run["halt_requested"] = True
        _run["phase"] = "halting"
    if pid:
        try:
            os.killpg(os.getpgid(pid), signal.SIGTERM)  # whole process group
        except (ProcessLookupError, PermissionError):
            pass
    return {"ok": True}


def run_status() -> dict:
    """Current maintenance state plus the tail of the run log."""
    with _run_lock:
        state = dict(_run)
    state.pop("pid", None)  # internal detail, not for the browser
    tail = ""
    progress = ""
    try:
        with open(LOG_PATH, "r", errors="replace") as f:
            raw = f.read()
        # Split tqdm's progress bar out of the log so it doesn't scroll it: the
        # latest bar line goes to the browser's path pane as one self-overwriting
        # line (`progress`), while everything else (headers, the === summary,
        # [committed]) stays in the log (`tail`). tqdm emits the bar either by
        # overwriting with \r or as fresh \n lines depending on the stream, so we
        # identify bar lines by content (the "NN%|" signature), not by \r.
        log_lines = []
        for line in raw.split("\n"):
            seg = line.rsplit("\r", 1)[-1]  # final \r overwrite, if any
            if _TQDM_RE.search(seg):
                progress = seg  # latest bar line wins → pinned in the path pane
            else:
                log_lines.append(line)
        tail = "\n".join(log_lines)[-6000:]
    except FileNotFoundError:
        pass
    state["log"] = tail
    state["progress"] = progress
    return state

PRESETS = [
    ("Row counts", "SELECT count(*) AS files FROM files"),
    (
        "Largest files",
        "SELECT path, size_bytes, mime_type\n"
        "FROM files ORDER BY size_bytes DESC LIMIT 100",
    ),
    (
        "Duplicate files (by MD5)",
        "SELECT md5, count(*) AS copies, sum(size_bytes) AS total_bytes,\n"
        "       min(path) AS example\n"
        "FROM files\n"
        "WHERE md5 IS NOT NULL\n"
        "GROUP BY md5 HAVING count(*) > 1\n"
        "ORDER BY total_bytes DESC LIMIT 200",
    ),
    (
        "Space by extension",
        "SELECT lower(extension) AS ext, count(*) AS files,\n"
        "       sum(size_bytes) AS total_bytes\n"
        "FROM files GROUP BY ext ORDER BY total_bytes DESC LIMIT 100",
    ),
    (
        "Space by volume",
        "SELECT volume, count(*) AS files, sum(size_bytes) AS total_bytes\n"
        "FROM files GROUP BY volume ORDER BY total_bytes DESC",
    ),
    (
        "Photos with GPS",
        "SELECT path, exif_camera_model, exif_gps_lat, exif_gps_lon,\n"
        "       exif_shoot_date\n"
        "FROM files\n"
        "WHERE exif_gps_lat IS NOT NULL\n"
        "ORDER BY exif_shoot_date DESC LIMIT 200",
    ),
    (
        "Crawl errors",
        "SELECT path, error_type, message, occurred_at FROM errors\n"
        "ORDER BY occurred_at DESC",
    ),
    ("Schema (files)", "DESCRIBE files"),
]


def run_query(sql: str) -> dict:
    """Run one read-only statement, returning columns/rows or an error."""
    with _lock:
        if _con is None:
            return {"error": "Database briefly unavailable (finishing a "
                             "maintenance swap) — try again in a moment."}
        try:
            cur = _con.cursor()
            cur.execute(sql)
            if cur.description is None:
                return {"columns": [], "rows": [], "truncated": False, "note": "OK"}
            columns = [d[0] for d in cur.description]
            rows = cur.fetchmany(MAX_ROWS + 1)
            truncated = len(rows) > MAX_ROWS
            rows = rows[:MAX_ROWS]
            # Make values JSON-safe (Decimal, datetime, bytes, etc.).
            safe = [[_jsonable(v) for v in row] for row in rows]
            return {"columns": columns, "rows": safe, "truncated": truncated}
        except Exception as exc:  # surface the DB error to the user
            return {"error": f"{type(exc).__name__}: {exc}"}


def _jsonable(v):
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, datetime):
        # Timestamps are stored naive-UTC (see crawler.ts_to_dt). Convert at the
        # display edge to the server's local time zone (DST-aware via the OS),
        # so result tables read in local wall-clock instead of UTC. (Hand-written
        # SQL that *filters* on a raw timestamp column still compares in UTC; the
        # Locate date filter converts explicitly — see locate().)
        return v.replace(tzinfo=timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, (bytes, bytearray)):
        return v.hex()
    return str(v)


_ql_proc = None  # the live Quick Look panel, so a new preview replaces it


def open_path(path, action="open") -> dict:
    """Open an indexed file (macOS default app), reveal it in Finder, or
    Quick Look it ("preview").

    Same threat model as the other POST handlers (localhost-only Host check,
    JSON content-type so cross-origin pages can't POST without a preflight).
    The path goes to `open`/`qlmanage` as an argv element — no shell — and
    only files that actually exist on disk are opened.
    """
    global _ql_proc
    if sys.platform != "darwin":
        return {"error": "Open is only supported on macOS."}
    if action not in ("open", "reveal", "preview"):
        return {"error": f"unknown action: {action}"}
    if not isinstance(path, str) or not path.startswith("/"):
        return {"error": "expected an absolute path"}
    if not os.path.exists(path):
        return {"error": "Not found on disk — volume offline, or index out of date?"}
    if action == "preview":
        # qlmanage blocks while its panel is open, so spawn detached; closing
        # the previous panel first keeps space-bar browsing snappy.
        if _ql_proc is not None and _ql_proc.poll() is None:
            _ql_proc.terminate()
        try:
            proc = subprocess.Popen(["qlmanage", "-p", path],
                                    stdout=subprocess.DEVNULL,
                                    stderr=subprocess.DEVNULL)
        except Exception as e:
            return {"error": f"Quick Look failed: {e}"}
        _ql_proc = proc
        # Reap the child whenever the user closes the panel, so a long-running
        # server doesn't accumulate zombies.
        threading.Thread(target=proc.wait, daemon=True).start()
        return {"ok": True}
    cmd = ["open", "-R", path] if action == "reveal" else ["open", path]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
    except Exception as e:
        return {"error": f"open failed: {e}"}
    if proc.returncode != 0:
        return {"error": (proc.stderr or "open failed").strip()}
    return {"ok": True}


def get_stats() -> dict:
    """Index-level stats for the status strip: row count, total bytes,
    per-volume counts with mounted state, and the last successful sync time
    (files.db's mtime — the file is only replaced when a run commits)."""
    if not os.path.exists(DB_PATH):
        return {"no_db": True}
    out = {"no_db": False}
    try:
        mtime = os.path.getmtime(DB_PATH)
        out["synced_at"] = (datetime.fromtimestamp(mtime, tz=timezone.utc)
                            .astimezone().strftime("%Y-%m-%d %H:%M"))
        out["synced_age_days"] = round(
            (datetime.now(tz=timezone.utc).timestamp() - mtime) / 86400, 1)
    except OSError:
        pass
    with _lock:
        if _con is None:
            out["error"] = "db briefly unavailable"
            return out
        try:
            cur = _con.cursor()
            n, total = cur.execute(
                "SELECT count(*), coalesce(sum(size_bytes), 0) FROM files"
            ).fetchone()
            vols = cur.execute(
                "SELECT volume, count(*) FROM files "
                "GROUP BY volume ORDER BY count(*) DESC").fetchall()
        except Exception as exc:
            out["error"] = f"{type(exc).__name__}: {exc}"
            return out
    out["files"] = n
    out["bytes"] = int(total)
    # Named volumes live under /Volumes, but the boot disk is stored as "/"
    # (the crawler walks "/" and records its paths plainly), so check that one
    # at "/". Use ismount — matching the crawler's logic — so a stale, empty
    # mountpoint directory doesn't read as "mounted".
    out["volumes"] = [
        {"name": v or "(unknown)", "files": c,
         "mounted": bool(v) and os.path.ismount("/" if v == "/" else "/Volumes/" + v)}
        for v, c in vols
    ]
    return out


PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Kendex</title>
<style>
  /* ============================================================
     Instrument-console theme.
     Two-temperature accent system on warm graphite:
       amber = writing the index (maintenance, progress, busy)
       cyan  = reading data (queries, locate, results)
       green = ready/committed   red = halt/destructive
     Motion budget: animate only the empty results pane and the
     live maintenance log — never while results are being read.
     ============================================================ */
  :root {
    color-scheme: dark;
    --bg: #121116;
    --panel: #1b1a21;
    --panel-2: #201f27;
    --field: #100f14;
    --line: rgba(255,255,255,.09);
    --line-soft: rgba(255,255,255,.055);
    --text: #eae7e1;
    --muted: #97928c;
    --amber: #e8a654;
    --amber-rgb: 232,166,84;
    --cyan: #6ec3d9;
    --cyan-rgb: 110,195,217;
    --green: #79c98c;
    --green-rgb: 121,201,140;
    --red: #e2606e;
    --red-rgb: 226,96,110;
    --mono: ui-monospace, "SF Mono", SFMono-Regular, Menlo, Consolas, monospace;
    --shadow: 0 18px 50px rgba(0,0,0,.3);
    /* surfaces (dark defaults; body.light overrides the lot) */
    --app-bg: radial-gradient(1100px 520px at 6% -10%, rgba(232,166,84,.055), transparent 60%),
              radial-gradient(900px 600px at 104% -4%, rgba(110,195,217,.05), transparent 55%),
              linear-gradient(165deg, #1a181d 0%, #141318 48%, #121116 100%);
    --side-bg: linear-gradient(180deg, #1c1a20 0%, #171619 60%, #151417 100%);
    --side-foot-bg: #151417;
    --side-line: rgba(255,255,255,.06);
    --rule: rgba(255,255,255,.12);
    --btn-face: #1d1c22;
    --btn-line: rgba(255,255,255,.055);
    --btn-text: #d8d5cf;
    --btn-shadow: inset 0 1px 0 rgba(255,255,255,.04), 0 1px 2px rgba(0,0,0,.35);
    --sync-text: #efd2a3;
    --halt-text: #e695a0;
    --save-text: #a8e3b8;
    --input-bg: linear-gradient(180deg, #131217, #0f0e13);
    --pane-bg: linear-gradient(180deg, #141318, #0e0d11);
    --pane-ring: linear-gradient(rgba(255,255,255,.08), rgba(255,255,255,.05));
    --pane-flat: #0e0d11;
    --grid-line: rgba(255,255,255,.03);
    --grid-line-2: rgba(255,255,255,.022);
    --hint: rgba(151,146,140,.7);
    --beam: linear-gradient(180deg,
        transparent 0%,
        rgba(110,195,217,.018) 55%,
        rgba(110,195,217,.05) 82%,
        rgba(160,222,238,.16) 98%,
        rgba(220,245,252,.28) 99.4%,
        transparent 100%);
    --th-bg: #191820;
    --th-text: #a39f98;
    --th-line: rgba(255,255,255,.1);
    --th-shadow: rgba(0,0,0,.4);
    --cell: #d6d3cc;
    --row-line: rgba(255,255,255,.045);
    --zebra: rgba(255,255,255,.015);
    --err: #f0939e;
    --strip-bg: linear-gradient(180deg, #15141a, #111016);
    --strip-text: #c9c6bf;
    --busy-text: #ddb077;
    --well: #0b0a0e;
    --modal-bg: linear-gradient(180deg, #201f26, #1a1920);
    --modal-btn: #232229;
    --scroll-thumb: rgba(255,255,255,.11);
    --scroll-thumb-hover: rgba(255,255,255,.2);
  }
  body.light {
    color-scheme: light;
    --panel: #efece4;
    --field: #fdfcf9;
    --line: rgba(45,35,18,.16);
    --line-soft: rgba(45,35,18,.1);
    --text: #2c2823;
    --muted: #7c766c;
    --amber: #b97b1e;
    --amber-rgb: 185,123,30;
    --cyan: #2e8aa6;
    --cyan-rgb: 46,138,166;
    --green: #3e9a58;
    --green-rgb: 62,154,88;
    --red: #c24350;
    --red-rgb: 194,67,80;
    --shadow: 0 14px 38px rgba(86,66,38,.16);
    --app-bg: radial-gradient(1100px 520px at 6% -10%, rgba(185,123,30,.07), transparent 60%),
              radial-gradient(900px 600px at 104% -4%, rgba(46,138,166,.06), transparent 55%),
              linear-gradient(165deg, #f7f4ed 0%, #f3f0e8 48%, #efece3 100%);
    --side-bg: linear-gradient(180deg, #f2efe7 0%, #ece9e0 60%, #e8e5dc 100%);
    --side-foot-bg: #e8e5dc;
    --side-line: rgba(45,35,18,.12);
    --rule: rgba(45,35,18,.18);
    --btn-face: #faf8f2;
    --btn-line: rgba(45,35,18,.14);
    --btn-text: #4a453c;
    --btn-shadow: inset 0 1px 0 rgba(255,255,255,.6), 0 1px 2px rgba(86,66,38,.12);
    --sync-text: #7c5410;
    --halt-text: #a83844;
    --save-text: #2c7a44;
    --input-bg: linear-gradient(180deg, #fffefb, #faf8f2);
    --pane-bg: linear-gradient(180deg, #fbf9f4, #f3f0e8);
    --pane-ring: linear-gradient(rgba(45,35,18,.2), rgba(45,35,18,.13));
    --pane-flat: #faf8f2;
    --grid-line: rgba(45,35,18,.055);
    --grid-line-2: rgba(45,35,18,.04);
    --hint: rgba(124,118,108,.85);
    --beam: linear-gradient(180deg,
        transparent 0%,
        rgba(46,138,166,.02) 55%,
        rgba(46,138,166,.05) 82%,
        rgba(46,138,166,.13) 98%,
        rgba(26,110,140,.22) 99.4%,
        transparent 100%);
    --th-bg: #edeade;
    --th-text: #6b655a;
    --th-line: rgba(45,35,18,.16);
    --th-shadow: rgba(86,66,38,.18);
    --cell: #38332b;
    --row-line: rgba(45,35,18,.08);
    --zebra: rgba(45,35,18,.025);
    --err: #b3424e;
    --strip-bg: linear-gradient(180deg, #f6f3ec, #f0ede4);
    --strip-text: #565047;
    --busy-text: #8a5f14;
    --well: #e6e2d6;
    --modal-bg: linear-gradient(180deg, #fbf9f4, #f2efe7);
    --modal-btn: #f3f0e8;
    --scroll-thumb: rgba(45,35,18,.2);
    --scroll-thumb-hover: rgba(45,35,18,.32);
  }
  * { box-sizing: border-box; }
  body {
    font: 14px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    margin: 0; display: flex; height: 100vh; color: var(--text);
    background: var(--app-bg);
  }
  ::selection { background: rgba(232,166,84,.32); }
  button, input, select, textarea { font: inherit; }
  button { color: inherit; }
  button:focus-visible { outline: 2px solid rgba(var(--cyan-rgb), .55); outline-offset: 2px; }
  ::-webkit-scrollbar { width: 10px; height: 10px; }
  ::-webkit-scrollbar-track { background: transparent; }
  ::-webkit-scrollbar-corner { background: transparent; }
  ::-webkit-scrollbar-thumb {
    background: var(--scroll-thumb); border-radius: 8px;
    border: 2px solid transparent; background-clip: padding-box;
  }
  ::-webkit-scrollbar-thumb:hover { background-color: var(--scroll-thumb-hover); }

  /* ---- sidebar ---- */
  #side {
    width: 252px; flex: none; display: flex; flex-direction: column;
    border-right: 1px solid var(--side-line);
    padding: 16px 14px 0; overflow-y: auto;
    background: var(--side-bg);
  }
  .brand { display: flex; gap: 11px; align-items: center; margin: 0 0 12px; flex: none; }
  .brand-mark {
    width: 36px; height: 36px; border-radius: 9px; display: grid; place-items: center;
    background: linear-gradient(145deg, #f4c178 0%, #dd9a44 55%, #b87425 100%);
    color: #221302; font: 700 13.5px var(--mono);
    box-shadow: 0 4px 14px rgba(232,166,84,.22), inset 0 1px 0 rgba(255,255,255,.45);
  }
  .brand-name { font-size: 15px; font-weight: 700; letter-spacing: -.01em; }
  .brand-meta {
    margin-top: 2px; font: 600 9.5px var(--mono);
    text-transform: uppercase; letter-spacing: .14em; color: var(--muted);
  }
  #side h3 {
    display: flex; align-items: center; gap: 8px; flex: none;
    margin: 14px 2px 7px; font: 600 10.5px var(--mono);
    text-transform: uppercase; letter-spacing: .16em; color: var(--muted);
  }
  #side h3::after {
    content: ""; flex: 1; height: 1px;
    background: linear-gradient(90deg, var(--rule), transparent);
  }
  #side h3:first-of-type { margin-top: 4px; }
  #side button {
    --accent-rgb: 151,146,140;            /* neutral; groups override below */
    position: relative; display: flex; align-items: center; gap: 9px;
    width: 100%; margin: 0 0 4px; min-height: 32px; padding: 6px 10px;
    text-align: left; cursor: pointer;
    border: 1px solid var(--btn-line); border-radius: 8px;
    background: linear-gradient(180deg, rgba(255,255,255,.035), rgba(255,255,255,0) 60%), var(--btn-face);
    color: var(--btn-text);
    box-shadow: var(--btn-shadow);
    overflow: hidden; isolation: isolate;
    transition: border-color .13s ease, background .13s ease, color .13s ease,
                transform .08s ease, box-shadow .13s ease;
  }
  #side button::before {                  /* accent tick */
    content: ""; flex: none; width: 7px; height: 7px; border-radius: 2px;
    background: rgba(var(--accent-rgb), .75);
    box-shadow: 0 0 6px rgba(var(--accent-rgb), .3);
    transition: background .13s ease, box-shadow .13s ease;
  }
  #side button:hover {
    border-color: rgba(var(--accent-rgb), .28);
    background: linear-gradient(180deg, rgba(var(--accent-rgb), .055), rgba(var(--accent-rgb), .015)), var(--btn-face);
  }
  #side button:hover::before {
    background: rgba(var(--accent-rgb), .95);
    box-shadow: 0 0 7px rgba(var(--accent-rgb), .35);
  }
  #side button:active {
    transform: translateY(1px);
    box-shadow: inset 0 2px 5px rgba(0,0,0,.35);
  }
  #side button:disabled { pointer-events: none; }
  #user-presets button { --accent-rgb: var(--cyan-rgb); }
  /* Sample queries collapsed into one dropdown (loads + runs on select). */
  #presets {
    width: 100%; margin: 0 0 4px; min-height: 34px; padding: 6px 30px 6px 11px;
    font: inherit; color: var(--btn-text); cursor: pointer;
    border: 1px solid var(--btn-line); border-radius: 8px;
    box-shadow: var(--btn-shadow); -webkit-appearance: none; appearance: none;
    background:
      url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='10' height='6'%3E%3Cpath fill='%23919691' d='M0 0l5 6 5-6z'/%3E%3C/svg%3E")
        no-repeat right 11px center,
      linear-gradient(180deg, rgba(255,255,255,.035), rgba(255,255,255,0) 60%), var(--btn-face);
  }
  #presets:hover { border-color: rgba(var(--cyan-rgb), .28); }
  /* Collapsible "Maintenance" group: rarely-used / destructive actions tucked
     away so the everyday Add Files action stays one click. */
  .maint-more { margin: 0 0 4px; }
  .maint-more > summary {
    list-style: none; display: flex; align-items: center; gap: 9px;
    min-height: 32px; padding: 6px 10px; margin: 0 0 4px; cursor: pointer;
    border: 1px solid var(--btn-line); border-radius: 8px; color: var(--btn-text);
    background: linear-gradient(180deg, rgba(255,255,255,.035), rgba(255,255,255,0) 60%), var(--btn-face);
    box-shadow: var(--btn-shadow);
  }
  .maint-more > summary::-webkit-details-marker { display: none; }
  .maint-more > summary::before {
    content: "\\25B8"; font-size: 10px; color: var(--muted); width: 7px; flex: none;
  }
  .maint-more[open] > summary::before { content: "\\25BE"; }
  .maint-more > summary:hover { border-color: rgba(var(--amber-rgb), .28); }
  /* Plain-language note demystifying the two "hash" buttons above it. */
  #maint .hint {
    margin: 1px 3px 9px; font-size: 11px; line-height: 1.5; color: var(--muted);
  }
  #maint .hint b { color: var(--btn-text); font-weight: 600; }
  #user-presets button .preset-del {
    margin-left: auto; padding: 0 5px; border-radius: 4px;
    color: var(--muted); opacity: 0; transition: opacity .12s ease;
  }
  #user-presets button:hover .preset-del { opacity: 1; }
  #user-presets button .preset-del:hover { color: var(--err); }
  #maint button { --accent-rgb: var(--amber-rgb); }
  #maint button[data-mode="sync"] {       /* the headline "do everything" action */
    border-color: rgba(var(--amber-rgb), .35);
    background: linear-gradient(180deg, rgba(var(--amber-rgb), .15), rgba(var(--amber-rgb), .04)), var(--btn-face);
    color: var(--sync-text); font-weight: 600;
  }
  #side-foot {
    /* Pinned to the bottom of the sidebar even when the button list scrolls,
       so the run status and Halt are always reachable. */
    position: sticky; bottom: 0; z-index: 2;
    margin: auto -14px 0; flex: none; padding: 11px 14px 13px;
    background: var(--side-foot-bg);
    border-top: 1px solid var(--side-line);
  }
  #side-status {
    display: flex; align-items: center; gap: 8px; padding: 0 2px 10px;
    font: 600 10.5px var(--mono); text-transform: uppercase;
    letter-spacing: .14em; color: var(--muted);
  }
  #side-status .dot {
    width: 7px; height: 7px; border-radius: 50%; flex: none;
    background: var(--green); box-shadow: 0 0 8px rgba(var(--green-rgb), .45);
  }
  #side-status .lbl-busy { display: none; color: var(--busy-text); }
  body.busy #side-status .lbl-idle { display: none; }
  body.busy #side-status .lbl-busy { display: inline; }
  body.busy #side-status .dot {
    background: var(--amber); box-shadow: 0 0 9px rgba(var(--amber-rgb), .6);
    animation: pulse 1.5s ease-in-out infinite;
  }
  #side #halt { --accent-rgb: var(--red-rgb); color: var(--halt-text); margin: 0; }
  body.busy #halt::before { animation: pulse 1.5s ease-in-out infinite; }
  #side #theme-toggle {
    width: 26px; min-height: 26px; margin: 0 0 0 auto; padding: 0;
    display: grid; place-items: center; flex: none;
    border: 1px solid transparent; border-radius: 6px;
    background: none; box-shadow: none; color: var(--muted); font-size: 13px;
  }
  #side #theme-toggle::before { content: none; }
  #side #theme-toggle:hover {
    border-color: var(--line); background: none; color: var(--text);
  }

  /* ---- main column ---- */
  #main {
    flex: 1; display: flex; flex-direction: column; padding: 20px 18px 16px;
    min-width: 0; position: relative;
  }
  #main::before {                         /* amber→cyan signature hairline */
    content: ""; position: absolute; top: 0; left: 18px; right: 18px; height: 2px;
    border-radius: 999px;
    background: linear-gradient(90deg, rgba(var(--amber-rgb), .65), rgba(var(--amber-rgb), .18) 38%,
                                rgba(var(--cyan-rgb), .4) 72%, transparent);
  }
  textarea {
    width: 100%; height: 114px; padding: 12px 14px; flex: none;
    border: 1px solid var(--line); border-radius: 9px;
    background: var(--input-bg);
    color: var(--text); caret-color: var(--cyan); resize: vertical;
    font: 13px/1.55 var(--mono); outline: none;
    box-shadow: inset 0 1px 3px rgba(0,0,0,.3);
    transition: border-color .12s ease, box-shadow .12s ease;
  }
  textarea:focus, input:focus, select:focus {
    border-color: rgba(var(--cyan-rgb), .6);
    box-shadow: 0 0 0 3px rgba(var(--cyan-rgb), .12);
  }
  #stats {
    display: flex; flex-wrap: wrap; gap: 4px 16px; align-items: center;
    margin: 0 0 8px; flex: none; min-height: 15px;
    font: 11px var(--mono); color: var(--muted); letter-spacing: .03em;
    cursor: default;
  }
  #stats .warn { color: var(--busy-text); }
  #bar { margin: 10px 0 8px; display: flex; gap: 10px; align-items: center; flex: none; }
  .bar-btn {
    min-height: 30px; padding: 4px 11px; border-radius: 7px; cursor: pointer;
    border: 1px solid var(--line); background: var(--modal-btn);
    color: var(--text); font-size: 12.5px;
    transition: border-color .12s ease;
  }
  .bar-btn:hover { border-color: var(--rule); }
  #status { margin-right: auto; }
  #filterbox {
    min-height: 30px; padding: 4px 11px; width: 170px;
    border: 1px solid var(--line); border-radius: 7px;
    background: var(--field); color: var(--text); outline: none;
    font-size: 12.5px; caret-color: var(--cyan);
    transition: border-color .12s ease, box-shadow .12s ease;
  }
  #export-csv { display: none; }
  #run, #locate button {
    display: inline-flex; align-items: center; gap: 8px;
    min-height: 34px; padding: 6px 16px; border-radius: 8px;
    border: 1px solid rgba(110,195,217,.7);
    background: linear-gradient(180deg, #8fd4e6, #58b1c8);
    color: #0a161a; font-weight: 650; cursor: pointer; letter-spacing: .01em;
    box-shadow: inset 0 1px 0 rgba(255,255,255,.4), 0 4px 14px rgba(110,195,217,.16);
    transition: filter .12s ease, transform .08s ease, box-shadow .12s ease;
  }
  #run:hover, #locate button:hover { filter: brightness(1.07); }
  #run:active, #locate button:active {
    transform: translateY(1px);
    box-shadow: inset 0 1px 0 rgba(255,255,255,.25), 0 2px 6px rgba(110,195,217,.14);
  }
  #run kbd {
    font: 600 10.5px var(--mono); padding: 2px 5px; border-radius: 4px;
    border: 1px solid rgba(10,22,26,.35); background: rgba(255,255,255,.25);
  }
  #status { font: 12px var(--mono); color: var(--muted); letter-spacing: .02em; }
  body.busy #status { color: var(--busy-text); }
  #run-progress {
    position: relative; height: 18px; flex: none; display: none; margin: 0 0 10px;
    border: 1px solid var(--line-soft); border-radius: 999px;
    overflow: hidden; background: var(--well);
  }
  #run-progress > div {
    height: 100%; width: 0%; border-radius: 999px;
    background: linear-gradient(90deg, #b87f33, #eab064 60%, #f3c98c);
    box-shadow: 0 0 12px rgba(232,166,84,.45);
    transition: width .25s ease;
  }
  #run-progress.indeterminate > div {
    /* No known total: a slow breathing amber wash instead of a flying bar. */
    width: 100%;
    background: linear-gradient(90deg, rgba(232,166,84,.22), rgba(232,166,84,.5), rgba(232,166,84,.22));
    box-shadow: none;
    animation: progress-breathe 3.2s ease-in-out infinite;
  }
  #progress-label {
    position: absolute; inset: 0; display: grid; place-items: center;
    font: 600 10.5px var(--mono); letter-spacing: .05em;
    color: #f3ead9; text-shadow: 0 1px 2px rgba(0,0,0,.55);
    pointer-events: none;
  }

  /* ---- motion ---- */
  @property --scan-angle {
    syntax: "<angle>";
    inherits: false;
    initial-value: 0deg;
  }
  @keyframes progress-breathe {
    0%, 100% { opacity: .5; }
    50% { opacity: 1; }
  }
  @keyframes scan-border {
    to { --scan-angle: 360deg; }
  }
  @keyframes grid-drift {
    from { background-position: 0 0, 0 0, 0 0; }
    to { background-position: 0 0, 0 36px, 36px 0; }
  }
  @keyframes scan-sweep {
    0% { background-position: 0 -180px; }
    72% { background-position: 0 calc(100% + 180px); }
    100% { background-position: 0 calc(100% + 180px); }
  }
  @keyframes pulse {
    0%, 100% { opacity: 1; }
    50% { opacity: .45; }
  }
  @keyframes settle-in {
    0% {
      border-color: rgba(var(--cyan-rgb), .55);
      box-shadow: 0 0 0 1px rgba(var(--cyan-rgb), .22), var(--shadow);
    }
    100% { border-color: var(--line); box-shadow: var(--shadow); }
  }

  /* ---- results pane: calm while reading, alive while idle ---- */
  #out {
    flex: 1; overflow: auto; position: relative; border-radius: 10px;
    border: 1px solid var(--line);
    background: var(--pane-bg);
    box-shadow: var(--shadow);
  }
  #out.settle { animation: settle-in .7s ease-out; }
  #out:empty {
    --scan-angle: 0deg;
    border-color: transparent;
    background:
      var(--pane-bg) padding-box,
      conic-gradient(from var(--scan-angle),          /* travelling comet */
        transparent 0deg,
        rgba(var(--amber-rgb), .03) 250deg,
        rgba(var(--amber-rgb), .4) 335deg,
        rgba(var(--cyan-rgb), .85) 357deg,
        transparent 360deg) border-box,
      var(--pane-ring) border-box;
    animation: scan-border 10s linear infinite;
  }
  #out:empty::before {                    /* drifting survey grid + idle hint */
    content: "ready — run a preset or type SQL";
    position: absolute; inset: 0; pointer-events: none;
    display: grid; place-items: center;
    font: 12px var(--mono); letter-spacing: .08em; color: var(--hint);
    background:
      radial-gradient(620px 320px at 50% 0%, rgba(var(--amber-rgb), .05), transparent 70%),
      repeating-linear-gradient(0deg, var(--grid-line) 0 1px, transparent 1px 36px),
      repeating-linear-gradient(90deg, var(--grid-line-2) 0 1px, transparent 1px 36px);
    animation: grid-drift 18s linear infinite;
  }
  #out:empty::after {                     /* slow scan beam */
    content: ""; position: absolute; inset: 0; pointer-events: none;
    border-radius: inherit;
    background: var(--beam) no-repeat;
    background-size: 100% 180px;
    animation: scan-sweep 7s cubic-bezier(.45,.05,.55,.95) infinite;
  }
  table { border-collapse: collapse; width: 100%; }
  th, td { padding: 7px 10px; text-align: left; white-space: nowrap;
           max-width: 480px; overflow: hidden; text-overflow: ellipsis; }
  th {
    position: sticky; top: 0; z-index: 1; background: var(--th-bg);
    color: var(--th-text); font: 600 11px var(--mono);
    text-transform: uppercase; letter-spacing: .07em;
    border-bottom: 1px solid var(--th-line);
    box-shadow: 0 1px 0 var(--th-shadow);
  }
  td {
    font: 12.5px/1.5 var(--mono); color: var(--cell);
    border-bottom: 1px solid var(--row-line);
  }
  td.num { text-align: right; }
  #out tr:nth-child(even) td { background: var(--zebra); }
  #out tr:hover td { background: rgba(var(--cyan-rgb), .1); }
  #out tr.sel td { background: rgba(var(--cyan-rgb), .18); }
  #out tr.offline td { opacity: .45; }
  .err { color: var(--err); padding: 14px 16px; white-space: pre-wrap;
         font: 12.5px/1.6 var(--mono); }

  /* ---- path readout strip (also hosts the tqdm progress line) ---- */
  #pathbox {
    flex: none; margin: 0 0 10px; padding: 8px 12px;
    border: 1px solid var(--line-soft); border-radius: 8px;
    background: var(--strip-bg);
    color: var(--strip-text); font: 12.5px/1.5 var(--mono);
    white-space: pre-wrap; word-break: break-all;
    box-shadow: inset 0 1px 3px rgba(0,0,0,.25);
    /* 2 text lines + padding + border (border-box): no wrap jiggle */
    min-height: calc(2.9em + 18px);
    transition: border-color .25s ease, color .25s ease;
  }
  #pathbox:empty::before {
    content: "hover a row to preview its path — right-click a row to open it";
    color: var(--hint);
  }
  body.busy #pathbox { border-color: rgba(var(--amber-rgb), .35); color: var(--busy-text); }

  /* ---- locate form ---- */
  #locate {
    border: 1px solid var(--line); border-radius: 10px; margin: 0 0 10px;
    padding: 10px 12px 12px; display: flex; flex-wrap: wrap; gap: 8px;
    align-items: center;
    background: linear-gradient(180deg, rgba(255,255,255,.025), rgba(255,255,255,0) 55%), var(--panel);
    box-shadow: inset 0 1px 0 rgba(255,255,255,.03);
  }
  #locate legend {
    display: flex; align-items: center; gap: 7px; padding: 0 8px;
    font: 600 10.5px var(--mono); text-transform: uppercase;
    letter-spacing: .16em; color: var(--muted);
  }
  #locate legend::before {
    content: ""; width: 6px; height: 6px; border-radius: 2px;
    background: var(--cyan); box-shadow: 0 0 7px rgba(var(--cyan-rgb), .5);
  }
  #locate input, #locate select {
    padding: 6px 9px; border: 1px solid var(--line); border-radius: 7px;
    background: var(--field); color: var(--text); outline: none;
    min-height: 34px; caret-color: var(--cyan);
    transition: border-color .12s ease, box-shadow .12s ease;
  }
  #locate input[type="checkbox"] { min-height: 0; accent-color: var(--cyan); }
  #locate label { display: flex; gap: 6px; align-items: center; color: var(--btn-text); }
  .utc-note { font-size: 12px; color: var(--muted); align-self: center; }

  /* ---- exclude-list modal ---- */
  #exmodal {
    position: fixed; inset: 0; z-index: 50;
    background: rgba(10,9,12,.55); backdrop-filter: blur(7px);
    display: flex; align-items: center; justify-content: center;
  }
  #exmodal[hidden] { display: none; }
  #exmodal-panel {
    position: relative;
    background: var(--modal-bg); color: var(--text);
    border: 1px solid var(--line); border-radius: 12px;
    padding: 20px; width: 580px; max-width: 92vw;
    max-height: 86vh; overflow: auto;
    box-shadow: 0 30px 80px rgba(0,0,0,.5);
  }
  #exmodal-panel h3 { margin: 0 0 6px; font-size: 15px; }
  #exmodal .ex-note { color: var(--muted); font-size: 13px; margin: 0 0 10px; }
  #ex-defaults {
    background: var(--field); border: 1px solid var(--line-soft); border-radius: 7px;
    padding: 8px 10px; color: var(--strip-text); max-height: 28vh; overflow: auto;
    white-space: pre-wrap; margin: 4px 0 12px; word-break: break-all;
    font: 12px/1.6 var(--mono);
  }
  #ex-user { width: 100%; height: 150px; box-sizing: border-box; }
  #exmodal .ex-btns { margin-top: 12px; display: flex; gap: 8px; align-items: center; }
  #exmodal .ex-btns button {
    padding: 7px 16px; border-radius: 7px; cursor: pointer;
    border: 1px solid var(--line); background: var(--modal-btn);
    transition: border-color .12s ease, background .12s ease;
  }
  #exmodal .ex-btns button:hover { border-color: var(--rule); }
  #ex-save {
    border-color: rgba(var(--green-rgb), .5); color: var(--save-text); font-weight: 600;
    background: linear-gradient(180deg, rgba(var(--green-rgb), .2), rgba(var(--green-rgb), .08));
  }
  #ex-save:hover { border-color: rgba(var(--green-rgb), .75); }
  #ex-msg { color: var(--muted); font-size: 13px; }

  /* ---- maintenance log: amber comet border while a run is live ---- */
  #log {
    flex: 1; overflow: auto; display: none; padding: 13px 15px;
    border: 1px solid var(--line); border-radius: 10px;
    background: var(--pane-flat); color: var(--cell); white-space: pre-wrap;
    font: 12.5px/1.55 var(--mono);
    box-shadow: var(--shadow);
  }
  body.busy #log {
    --scan-angle: 0deg;
    border-color: transparent;
    background:
      linear-gradient(var(--pane-flat), var(--pane-flat)) padding-box,
      conic-gradient(from var(--scan-angle),
        transparent 0deg,
        rgba(var(--amber-rgb), .05) 230deg,
        rgba(var(--amber-rgb), .55) 340deg,
        rgba(var(--amber-rgb), .9) 357deg,
        transparent 360deg) border-box,
      linear-gradient(rgba(var(--amber-rgb), .18), rgba(var(--amber-rgb), .06)) border-box;
    animation: scan-border 4s linear infinite;
  }
  /* ---- row inspector ---- */
  #inspect {
    flex: none; max-height: 200px; overflow: auto; margin: 10px 0 0;
    border: 1px solid var(--line); border-radius: 10px; padding: 10px 14px;
    background: var(--strip-bg);
  }
  #inspect[hidden] { display: none; }
  #inspect-head {
    display: flex; align-items: center; gap: 8px; margin: 0 0 8px;
  }
  #inspect-title {
    font: 600 12px var(--mono); color: var(--text);
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
    margin-right: auto;
  }
  #inspect-head button {
    min-height: 24px; padding: 2px 9px; border-radius: 6px; cursor: pointer;
    border: 1px solid var(--line); background: var(--modal-btn);
    color: var(--text); font-size: 11.5px;
  }
  #inspect-head button:hover { border-color: rgba(var(--cyan-rgb), .5); }
  #inspect-grid {
    display: grid; grid-template-columns: max-content 1fr; gap: 3px 16px;
    font: 12px/1.5 var(--mono);
  }
  #inspect-grid .k { color: var(--muted); }
  #inspect-grid .v { color: var(--cell); word-break: break-all; }

  /* ---- first run: no database yet ---- */
  body.nodb #out:empty::before {
    content: "no index yet — review Edit exclude list, then run Scan for new (the first scan can take hours)";
  }

  /* ---- duplicate manager ---- */
  #dupmodal {
    position: fixed; inset: 0; z-index: 50;
    background: rgba(10,9,12,.55); backdrop-filter: blur(7px);
    display: flex; align-items: center; justify-content: center;
  }
  #dupmodal[hidden] { display: none; }
  #dupmodal-panel {
    position: relative;
    background: var(--modal-bg); color: var(--text);
    border: 1px solid var(--line); border-radius: 12px;
    padding: 20px; width: 820px; max-width: 94vw;
    max-height: 88vh; overflow: auto; display: flex; flex-direction: column;
    box-shadow: 0 30px 80px rgba(0,0,0,.5);
  }
  #dupmodal-panel h3 { margin: 0 0 6px; font-size: 15px; }
  #dup-tools {
    display: flex; gap: 8px; align-items: center; margin: 0 0 10px; flex: none;
  }
  #dup-summary { font: 11.5px var(--mono); color: var(--muted); margin-left: auto; }
  #dup-list {
    flex: 1; overflow: auto; border: 1px solid var(--line-soft);
    border-radius: 8px; background: var(--field); padding: 4px 10px;
    font: 12px/1.6 var(--mono); min-height: 120px;
  }
  .dup-head {
    margin: 10px 0 4px; padding-top: 8px; border-top: 1px solid var(--line-soft);
    color: var(--text); font-weight: 600;
  }
  .dup-head:first-child { border-top: none; margin-top: 2px; }
  .dup-head .waste { color: var(--busy-text); font-weight: 400; }
  .dup-row { display: flex; gap: 8px; align-items: baseline; }
  .dup-row input[type="checkbox"] { accent-color: var(--red); }
  .dup-row .p { word-break: break-all; color: var(--cell); }
  .dup-row .m { color: var(--muted); flex: none; }
  .dup-row input:checked ~ .p { text-decoration: line-through; opacity: .6; }

  /* ---- row context menu (Open / Reveal / Copy) ---- */
  #ctxmenu {
    position: fixed; z-index: 60; min-width: 180px; padding: 4px;
    background: var(--modal-bg); border: 1px solid var(--line);
    border-radius: 9px; box-shadow: 0 14px 40px rgba(0,0,0,.35);
  }
  #ctxmenu[hidden] { display: none; }
  #ctxmenu button {
    display: block; width: 100%; padding: 6px 10px; text-align: left;
    border: none; border-radius: 6px; background: none;
    font-size: 13px; cursor: pointer; color: var(--text);
  }
  #ctxmenu button:hover { background: rgba(var(--cyan-rgb), .14); }
  .modal-x {
    position: absolute; top: 12px; right: 12px; width: 28px; height: 28px;
    display: grid; place-items: center; cursor: pointer;
    border: 1px solid transparent; border-radius: 7px;
    background: none; color: var(--muted); font-size: 13px;
  }
  .modal-x:hover { border-color: var(--line); color: var(--text); }
  button:disabled { opacity: .38; cursor: not-allowed; }
  @media (prefers-reduced-motion: reduce) {
    *, *::before, *::after {
      animation-duration: .01ms !important;
      animation-iteration-count: 1 !important;
      transition-duration: .01ms !important;
    }
  }
</style>
</head>
<body>
<div id="side">
  <div class="brand">
    <div class="brand-mark">K</div>
    <div>
      <div class="brand-name">Kendex</div>
      <div class="brand-meta">Local index console</div>
    </div>
  </div>
  <h3>Queries</h3>
  <select id="presets" aria-label="Sample queries">
    <option value="" disabled selected>Sample queries&hellip;</option>
  </select>
  <div id="user-presets"></div>
  <h3>Indexing</h3>
  <div id="maint">
    <button data-mode="add" title="Finds everything new on your drives, adds it to the index, and checks the new files for duplicates. Safe to stop and resume — nothing already added is lost.">Add Files</button>
    <p class="hint"><b>Add Files</b> finds everything new on your drives, adds it
      to the index, and checks the new files for duplicates. When it finishes,
      the log reports any duplicate sets found &mdash; open the
      <b>Duplicate manager</b> to review them and choose which copy to delete,
      if any.</p>
    <details class="maint-more">
      <summary>Maintenance</summary>
      <button data-mode="scan">Scan for new</button>
      <button data-mode="reindex">Reindex changed</button>
      <button data-mode="prune">Prune deleted</button>
      <button data-mode="prune_excluded">Prune excluded</button>
      <button data-mode="sync">Full sync (all 3)</button>
      <button data-mode="compact">Compact DB</button>
    </details>
  </div>
  <h3>Tools</h3>
  <div id="tools">
    <button id="dupes-open">Duplicate manager</button>
    <button id="edit-excludes">Edit exclude list</button>
    <button id="clearlog">Clear output</button>
  </div>
  <div id="side-foot">
    <div id="side-status">
      <span class="dot"></span>
      <span class="lbl-idle">Ready</span>
      <span class="lbl-busy">Indexing&hellip;</span>
      <button id="theme-toggle" title="Switch light / dark theme">&#9680;</button>
    </div>
    <button id="halt" disabled>&#9632; Halt &amp; discard run</button>
  </div>
</div>
<div id="main">
  <div id="stats" title="Click to refresh"></div>
  <fieldset id="locate">
    <legend>Locate files</legend>
    <select id="loc-kind">
      <option value="file">file name</option>
      <option value="folder">folder name</option>
      <option value="either">either (path)</option>
    </select>
    <input id="loc-name" size="22" placeholder="contains&hellip;">
    <label><input type="checkbox" id="loc-regex"> regex</label>
    <input id="loc-ext" size="14" placeholder="ext: jpg, png">
    <label>created <input id="loc-from" type="date"> &ndash;
      <input id="loc-to" type="date"></label>
    <span class="utc-note" title="Stored in UTC, displayed in this machine's local time zone (Pacific)">times shown in local time</span>
    <select id="loc-volmode">
      <option value="in">volume is</option>
      <option value="notin">volume is not</option>
    </select>
    <input id="loc-vol" size="18" placeholder="TB5_DOCK8, OWC HD1" list="vol-list">
    <datalist id="vol-list"></datalist>
    <button id="loc-go">Locate &#9654;</button>
  </fieldset>
  <textarea id="sql" spellcheck="false"
    placeholder="SELECT * FROM files LIMIT 100"></textarea>
  <div id="bar">
    <button id="run" title="Ctrl/Cmd + Enter">Run query <kbd>&#8984;&#9166;</kbd></button>
    <button id="savequery" class="bar-btn" title="Save the SQL box as a sidebar preset">Save query</button>
    <span id="status"></span>
    <input id="filterbox" placeholder="filter results&hellip;" title="Narrow the loaded rows (client-side)">
    <button id="export-csv" class="bar-btn" title="Download the visible rows as CSV">Export CSV</button>
  </div>
  <div id="run-progress" aria-hidden="true"><div></div><span id="progress-label"></span></div>
  <div id="pathbox"></div>
  <div id="out"></div>
  <div id="inspect" hidden>
    <div id="inspect-head">
      <span id="inspect-title"></span>
      <button data-act="open">Open</button>
      <button data-act="preview">Quick Look</button>
      <button data-act="reveal">Reveal</button>
      <button data-act="copy">Copy path</button>
      <button id="inspect-dupes" hidden>List copies</button>
      <button type="button" id="inspect-close" title="Close" aria-label="Close">&#10005;</button>
    </div>
    <div id="inspect-grid"></div>
  </div>
  <pre id="log"></pre>
</div>
<div id="exmodal" hidden>
  <div id="exmodal-panel">
    <button type="button" class="modal-x" id="ex-x" title="Close" aria-label="Close">&#10005;</button>
    <h3>Crawler exclude list</h3>
    <p class="ex-note">Paths the crawler skips. An absolute path excludes that
      folder and everything under it. A pattern with <code>*</code> (any text)
      or <code>?</code> (one character) matches the whole path &mdash; add a
      trailing <code>*</code> to catch a folder's contents, e.g.
      <code>*/Library/Application&nbsp;Support/*</code>.
      Changes take effect on the <b>next</b> crawl, not retroactively.</p>
    <div><b>Built-in (always excluded)</b></div>
    <pre id="ex-defaults"></pre>
    <div><b>Your excludes</b> &mdash; one path or pattern per line:</div>
    <textarea id="ex-user" spellcheck="false"
      placeholder="/Volumes/SomeVolume&#10;*/Library/Application Support/*"></textarea>
    <div class="ex-btns">
      <button id="ex-save">Save</button>
      <button id="ex-cancel">Cancel</button>
      <span id="ex-msg"></span>
    </div>
  </div>
</div>
<div id="ctxmenu" hidden>
  <button data-act="open">Open</button>
  <button data-act="preview">Quick Look</button>
  <button data-act="reveal">Reveal in Finder</button>
  <button data-act="copy">Copy path</button>
</div>
<div id="dupmodal" hidden>
  <div id="dupmodal-panel">
    <button type="button" class="modal-x" id="dup-x" title="Close" aria-label="Close">&#10005;</button>
    <h3>Duplicate manager</h3>
    <p class="ex-note">Top duplicate groups by wasted space (largest first).
      Tick the copies you want to delete &mdash; at least one copy of every file
      must stay unticked. Export produces a path list to review and feed to
      <b>rm</b> / <b>xargs</b>; Kendex never deletes anything itself.</p>
    <div id="dup-tools">
      <button id="dup-newest" class="bar-btn">Keep newest in every group</button>
      <button id="dup-clear" class="bar-btn">Clear all marks</button>
      <span id="dup-summary"></span>
    </div>
    <div id="dup-list"></div>
    <div class="ex-btns">
      <button id="dup-export">Export deletion list</button>
      <button id="dup-close">Close</button>
    </div>
  </div>
</div>
<script>
const PRESETS = __PRESETS__;
const sql = document.getElementById('sql');
const out = document.getElementById('out');
const status = document.getElementById('status');
const pathbox = document.getElementById('pathbox');
const runProgress = document.getElementById('run-progress');
const runProgressBar = runProgress.firstElementChild;

// ---- Theme: follow the system by default; remember a manual override ----
const themeBtn = document.getElementById('theme-toggle');
function applyTheme(t) { document.body.classList.toggle('light', t === 'light'); }
let theme = localStorage.getItem('kendexTheme')
  || (matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark');
applyTheme(theme);
themeBtn.onclick = () => {
  theme = (theme === 'light') ? 'dark' : 'light';
  localStorage.setItem('kendexTheme', theme);
  applyTheme(theme);
};

const presetsEl = document.getElementById('presets');
for (const [name, q] of PRESETS) {
  const o = document.createElement('option');
  o.value = name; o.textContent = name;
  presetsEl.appendChild(o);
}
presetsEl.onchange = () => {
  const hit = PRESETS.find(([name]) => name === presetsEl.value);
  if (hit) { sql.value = hit[1]; run(); }
  presetsEl.selectedIndex = 0;   // snap back to the "Sample queries…" label
};

function isNum(v){ return typeof v === 'number'; }

let lastData = null;          // most recent result set, for re-sorting in place
let sortCol = -1, sortDir = 0;   // dir: -1 desc, +1 asc, 0 unsorted
let offlineVols = new Set();  // indexed volumes that aren't mounted right now
let selTr = null;             // currently selected result row
const exportBtn = document.getElementById('export-csv');
const filterBox = document.getElementById('filterbox');

// Compare two cell values: numeric if both numbers, else case-insensitive
// natural string order. Nulls always sort last, regardless of direction.
function cellCmp(a, b, dir){
  if (a === null && b === null) return 0;
  if (a === null) return 1;
  if (b === null) return -1;
  const base = (typeof a === 'number' && typeof b === 'number')
    ? a - b
    : String(a).localeCompare(String(b), undefined, {numeric: true, sensitivity: 'base'});
  return dir * base;
}

function sortBy(col){
  // First click on a column: descending; click again toggles.
  if (col === sortCol) sortDir = -sortDir; else { sortCol = col; sortDir = -1; }
  lastData.rows.sort((r1, r2) => cellCmp(r1[col], r2[col], sortDir));
  renderTable();
}

function fmtBytes(n){
  if (typeof n !== 'number' || !isFinite(n)) return n;
  if (n < 1024) return n + ' B';
  const u = ['KB','MB','GB','TB','PB'];
  let v = n, i = -1;
  do { v /= 1024; i++; } while (v >= 1024 && i < u.length - 1);
  return v.toFixed(v >= 100 ? 0 : 1) + ' ' + u[i];
}
const isByteCol = c => /bytes|size/i.test(c);
const DATETIME_RE = /^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$/;

function renderTable(){
  const data = lastData;
  out.innerHTML = '';
  const t = document.createElement('table');
  const thead = document.createElement('tr');
  data.columns.forEach((c, i) => {
    const th = document.createElement('th');
    const arrow = i === sortCol ? (sortDir < 0 ? ' ▼' : ' ▲') : '';
    th.textContent = c + arrow;
    th.style.cursor = 'pointer';
    th.title = 'Click to sort';
    th.onclick = () => sortBy(i);
    thead.appendChild(th);
  });
  t.appendChild(thead);
  const volIdx = data.columns.indexOf('volume');
  for (const row of data.rows) {
    const tr = document.createElement('tr');
    // Show the row's path in the readout pane on hover. Works for any column
    // holding a path (path, example, etc.): first cell that looks like one.
    const p = row.find(v => typeof v === 'string' && v.startsWith('/'));
    tr._row = row;
    tr._path = p;
    tr._txt = row.map(v => v === null ? '' : String(v)).join(' ').toLowerCase();
    if (p !== undefined) {
      tr.onmouseenter = () => { pathbox.textContent = p; };
      tr.oncontextmenu = e => { selectRow(tr); showCtx(e, p); };
    }
    tr.onclick = () => selectRow(tr === selTr ? null : tr);
    // Dim rows whose volume isn't currently mounted (set by /api/stats).
    const vol = (volIdx !== -1 && typeof row[volIdx] === 'string') ? row[volIdx]
      : (p && p.startsWith('/Volumes/') ? p.split('/')[2] : null);
    if (vol && offlineVols.has(vol)) {
      tr.classList.add('offline');
      tr.title = 'volume "' + vol + '" is not mounted';
    }
    row.forEach((v, i) => {
      const td = document.createElement('td');
      if (isNum(v)) td.className = 'num';
      if (v === null) {
        td.textContent = '∅';
      } else if (isNum(v) && isByteCol(data.columns[i])) {
        td.textContent = fmtBytes(v);            // raw value stays in the title
        td.title = v.toLocaleString() + ' bytes';
      } else if (typeof v === 'string' && DATETIME_RE.test(v)) {
        td.textContent = v.slice(0, 16);         // drop :seconds for scanning
        td.title = v;
      } else {
        td.textContent = v;
      }
      tr.appendChild(td);
    });
    t.appendChild(tr);
  }
  out.appendChild(t);
  selectRow(null);
  exportBtn.style.display = data.rows.length ? '' : 'none';
  // Retrigger the brief border settle-flash so each new result set lands
  // with a pulse, then the pane goes still for reading.
  out.classList.remove('settle');
  void out.offsetWidth;
  out.classList.add('settle');
  applyFilter();
}

function updateStatusCount(shown){
  if (!lastData) return;
  const total = lastData.rows.length;
  let txt = (shown === total) ? total + ' rows' : shown + ' of ' + total + ' rows';
  if (lastData.truncated) txt += ' (capped at ' + __MAX_ROWS__ + ')';
  status.textContent = txt;
}

// Client-side narrowing of the loaded rows (matches raw cell values).
function applyFilter(){
  if (!lastData) return;
  const q = filterBox.value.trim().toLowerCase();
  let shown = 0;
  const trs = out.querySelectorAll('table tr');
  trs.forEach((tr, i) => {
    if (i === 0) return;  // header row
    const hit = !q || tr._txt.includes(q);
    tr.style.display = hit ? '' : 'none';
    if (hit) shown++;
    else if (tr === selTr) selectRow(null);
  });
  updateStatusCount(shown);
}
filterBox.oninput = applyFilter;

async function run() {
  pushHistory(sql.value);
  status.textContent = 'Running…';
  out.innerHTML = '';
  selectRow(null);
  exportBtn.style.display = 'none';
  let data;
  try {
    const r = await fetch('/api/query', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({sql: sql.value})
    });
    data = await r.json();
  } catch (e) { status.textContent = 'Request failed: ' + e; return; }

  if (data.error) {
    status.textContent = '';
    const d = document.createElement('div');
    d.className = 'err'; d.textContent = data.error;
    out.appendChild(d);
    return;
  }
  if (data.note && !data.columns.length) {
    status.textContent = data.note; return;
  }
  lastData = data;
  sortCol = -1; sortDir = 0;   // new result set starts unsorted (DB order)
  renderTable();
}

document.getElementById('run').onclick = run;
sql.addEventListener('keydown', e => {
  if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') { e.preventDefault(); run(); }
  else if ((e.ctrlKey || e.metaKey) && (e.key === 'ArrowUp' || e.key === 'ArrowDown')) {
    if (!sqlHist.length) return;   // Cmd+Up/Down cycles past queries
    e.preventDefault();
    histIdx = (e.key === 'ArrowUp')
      ? Math.min(histIdx + 1, sqlHist.length - 1)
      : Math.max(histIdx - 1, -1);
    sql.value = histIdx === -1 ? '' : sqlHist[sqlHist.length - 1 - histIdx];
  }
});

// ---- Locate Files: build SQL from the form, show it in the box, run it ----
function sqlStr(s) { return "'" + s.replace(/'/g, "''") + "'"; }

// Clicking a row with an md5: fetch every path sharing that md5 and offer the
// list as a downloadable text file (one path per line) for xargs-style pruning.
async function dupList(md5){
  let data;
  try {
    const r = await fetch('/api/query', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({sql:
        'SELECT path FROM files WHERE md5 = ' + sqlStr(md5) + ' ORDER BY path'})
    });
    data = await r.json();
  } catch (e) { alert('Lookup failed: ' + e); return; }
  if (data.error) { alert(data.error); return; }
  const paths = data.rows.map(row => row[0]);
  if (paths.length < 2) { alert('Only one copy of this file is indexed.'); return; }
  const cap = data.truncated
    ? '\\n\\nNOTE: capped at ' + paths.length + ' paths — this md5 has MORE ' +
      'copies than that, so the list is incomplete (re-run after deleting).'
    : '';
  if (!confirm('Found ' + paths.length + (data.truncated ? '+' : '') +
      ' copies of this file (md5 ' + md5.slice(0, 8) + '…).' + cap +
      '\\n\\nDownload the path list as a text file?' +
      '\\n\\nThe file lists EVERY copy — edit it down before feeding it to ' +
      'rm, or you will delete the last copy too.')) return;
  const blob = new Blob([paths.join('\\n') + '\\n'], {type: 'text/plain'});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'dupes_' + md5.slice(0, 8) + '.txt';
  a.click();
  // Deferred: revoking synchronously can cancel the download in Chromium.
  setTimeout(() => URL.revokeObjectURL(a.href), 10000);
}
const locName = document.getElementById('loc-name');
const locExt  = document.getElementById('loc-ext');
const locFrom = document.getElementById('loc-from');
const locTo   = document.getElementById('loc-to');
const locVol  = document.getElementById('loc-vol');

function locate() {
  const conds = [];
  const name = locName.value.trim();
  if (name) {
    // What the name matches against: the filename, the folder part of the
    // path (= files inside a matching folder; the index has no folder rows),
    // or the whole path.
    const kind = document.getElementById('loc-kind').value;
    const target = kind === 'file' ? 'filename'
      : kind === 'folder' ? "regexp_replace(path, '/[^/]*$', '')"
      : 'path';
    if (document.getElementById('loc-regex').checked) {
      conds.push('regexp_matches(' + target + ', ' + sqlStr(name) + ", 'i')");
    } else {
      // Plain substring match: escape LIKE wildcards so they match literally.
      const esc = name.replace(/[\\\\%_]/g, m => '\\\\' + m);
      conds.push(target + ' ILIKE ' + sqlStr('%' + esc + '%') + " ESCAPE '\\\\'");
    }
  }
  const exts = locExt.value.split(/[,\\s]+/).filter(Boolean)
    .map(e => '.' + e.replace(/^\\./, '').toLowerCase());  // stored as ".jpg"
  if (exts.length)
    conds.push('extension IN (' + exts.map(sqlStr).join(', ') + ')');
  // created_at is stored naive-UTC; convert to local wall-clock before comparing
  // so "created June 8" means the local day, not the UTC day (DST handled by the
  // named zone). Display in the results table is localized the same way server-side.
  const createdLocal = "timezone('America/Los_Angeles', timezone('UTC', created_at))";
  if (locFrom.value)
    conds.push(createdLocal + ' >= DATE ' + sqlStr(locFrom.value));
  if (locTo.value)  // end date inclusive
    conds.push(createdLocal + ' < DATE ' + sqlStr(locTo.value) + ' + INTERVAL 1 DAY');
  const vols = locVol.value.split(',').map(v => v.trim()).filter(Boolean);
  if (vols.length) {
    const op = document.getElementById('loc-volmode').value === 'in'
      ? 'IN' : 'NOT IN';
    conds.push('volume ' + op + ' (' + vols.map(sqlStr).join(', ') + ')');
  }
  if (!conds.length) { alert('Enter at least one search criterion.'); return; }
  sql.value = 'SELECT path, size_bytes, created_at, modified_at, mime_type\\n'
    + 'FROM files\\nWHERE ' + conds.join('\\n  AND ') + '\\nORDER BY path';
  if (!runActive) {  // make sure the results pane is the one showing
    log.style.display = 'none';
    out.style.display = 'block';
  }
  run();
}

document.getElementById('loc-go').onclick = locate;
for (const el of [locName, locExt, locFrom, locTo, locVol])
  el.addEventListener('keydown', e => {
    if (e.key === 'Enter') { e.preventDefault(); locate(); }
  });

// ---- Row context menu: open / reveal / copy a result's file path ----
const ctxMenu = document.getElementById('ctxmenu');
let ctxPath = null;

function showCtx(e, p) {
  e.preventDefault();
  ctxPath = p;
  ctxMenu.hidden = false;  // unhide first so the size can be measured
  const r = ctxMenu.getBoundingClientRect();
  ctxMenu.style.left = Math.min(e.clientX, innerWidth - r.width - 8) + 'px';
  ctxMenu.style.top = Math.min(e.clientY, innerHeight - r.height - 8) + 'px';
}
function hideCtx() { ctxMenu.hidden = true; }
document.addEventListener('click', hideCtx);
document.addEventListener('keydown', e => {
  if (e.key !== 'Escape') return;
  hideCtx();
  exModal.hidden = true;     // every dialog closes on Escape, not just Cancel
  dupModal.hidden = true;
});
out.addEventListener('scroll', hideCtx);

async function fileAction(path, action){
  let d;
  try {
    const r = await fetch('/api/open', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({path: path, action: action})
    });
    d = await r.json();
  } catch (err) { alert('Request failed: ' + err); return; }
  if (d.error) alert(d.error);
}
function copyPath(p){
  navigator.clipboard.writeText(p).then(
    () => { status.textContent = 'Path copied.'; },
    () => { prompt('Copy the path:', p); });
}
ctxMenu.onclick = (e) => {
  const act = e.target.dataset && e.target.dataset.act;
  if (!act || !ctxPath) return;
  hideCtx();
  if (act === 'copy') copyPath(ctxPath);
  else fileAction(ctxPath, act);
};

// ---- Maintenance (runs crawler.py against a disposable copy of files.db) ----
const log = document.getElementById('log');
const maintBtns = [...document.querySelectorAll('#maint button')];
const haltBtn = document.getElementById('halt');
const clearBtn = document.getElementById('clearlog');
const LABELS = {reindex:'Reindex changed', scan:'Scan for new',
                add:'Add Files',
                prune:'Prune deleted', prune_excluded:'Prune excluded',
                sync:'Full sync', compact:'Compact DB'};
const DIRECT_MODES = ['add'];   // write straight to the DB; halting keeps progress
const isDirect = m => DIRECT_MODES.includes(m);
let runMode = null;             // current/last run mode, for halt messaging
const PHASES = {preparing:'snapshotting files.db', running:'running',
                halting:'halting'};
let polling = null;
let runActive = false;
let logAnchor = null;  // text at the moment Clear was clicked; render only what follows

function renderLog(full) {
  let text = full || '';
  if (logAnchor) {
    const i = text.lastIndexOf(logAnchor);
    // Anchor found → show only what came after the clear. Not found → the
    // tail rolled past the clear point (or a new run rewrote the log), so
    // everything visible is new.
    if (i !== -1) text = text.slice(i + logAnchor.length);
  } else if (!text) {
    text = '(starting…)';
  }
  log.textContent = text;
}

for (const b of maintBtns) {
  b.onclick = async () => {
    const m = b.dataset.mode;
    const msg = isDirect(m)
      ? 'Run "' + LABELS[m] + '"?\\n\\nIt adds new files to the index and is safe '
        + 'to halt and resume — nothing already added is lost.'
      : 'Run "' + LABELS[m] + '"?\\n\\nThe task works on a copy of files.db; your '
        + 'current DB is only replaced if it finishes. You can Halt & discard at '
        + 'any time.';
    if (!confirm(msg)) return;
    const r = await fetch('/api/run', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({mode: b.dataset.mode})
    });
    const d = await r.json();
    if (d.error) { alert(d.error); return; }
    logAnchor = null;  // new run, new log — show it from the start
    showLog(); poll();
  };
}

haltBtn.onclick = async () => {
  const hmsg = isDirect(runMode)
    ? 'Stop Add Files?\\n\\nFiles added so far are kept — click Add Files again '
      + 'later to resume where it left off.'
    : 'Halt the running scan and discard its in-progress work?\\n\\n'
      + 'Your current files.db is left completely unchanged.';
  if (!confirm(hmsg)) return;
  haltBtn.disabled = true;
  const d = await (await fetch('/api/run/halt', {method: 'POST',
    headers: {'Content-Type': 'application/json'}})).json();
  if (d.error) { alert(d.error); }
};

clearBtn.onclick = () => {
  // Blank whatever is showing: query results, path readout, and the log window.
  out.innerHTML = '';
  status.textContent = '';
  pathbox.textContent = '';
  lastData = null;
  filterBox.value = '';
  exportBtn.style.display = 'none';
  selectRow(null);
  logAnchor = log.textContent ? log.textContent.slice(-300) : null;
  log.textContent = '';
  if (!runActive) {  // nothing scrolling — return to the (now blank) results pane
    log.style.display = 'none';
    out.style.display = 'block';
  }
  // If a run is active, the log stays visible and refills from the top with
  // only the lines produced after this click (see renderLog/logAnchor).
};

function showLog() {
  out.style.display = 'none';
  log.style.display = 'block';
}
function setBusy(on) {
  maintBtns.forEach(b => b.disabled = on);  // can't start a second run
  haltBtn.disabled = !on;                   // halt only while a run is active
  // Drives the sidebar status dot and the amber "live run" accents in CSS.
  document.body.classList.toggle('busy', on);
  // The SQL Run button stays enabled — queries work against the live DB during
  // a run (the crawler is writing to a separate copy).
}

const progressLabel = document.getElementById('progress-label');
function updateProgress(active, progressLine) {
  if (!active) {
    runProgress.style.display = 'none';
    runProgress.classList.remove('indeterminate');
    runProgressBar.style.width = '0%';
    progressLabel.textContent = '';
    return;
  }
  runProgress.style.display = 'block';
  const line = progressLine || '';
  const pct = line.match(/(\\d{1,3})%\\|/);
  if (pct) {
    // Known total (reindex/verify): a real percentage fill.
    const p = Math.max(0, Math.min(100, Number(pct[1])));
    runProgress.classList.remove('indeterminate');
    runProgressBar.style.width = p + '%';
    progressLabel.textContent = p + '%';
    return;
  }
  // No total (first scan): tqdm emits "917601file [3:55:20, 55.64file/s, …" —
  // surface the real numbers instead of a meaningless animation.
  runProgress.classList.add('indeterminate');
  runProgressBar.style.width = '';
  const count = line.match(/(\\d+)file \\[([0-9:]+), ([0-9.]+)file\\/s/);
  progressLabel.textContent = count
    ? Number(count[1]).toLocaleString() + ' files · ' + count[3] + '/s · '
      + count[2] + ' elapsed'
    : 'working…';
}

async function poll() {
  if (polling) clearInterval(polling);
  const tick = async () => {
    let s;
    try { s = await (await fetch('/api/run/status')).json(); }
    catch (e) { return; }
    runActive = s.active;
    renderLog(s.log);
    updateProgress(s.active, s.progress);
    log.scrollTop = log.scrollHeight;
    // tqdm progress bar (if any) overwrites a single line in the path pane
    // rather than scrolling the log. Only update when there's a live line, so
    // the final percentage stays pinned after the bar closes.
    if (s.progress) pathbox.textContent = s.progress;
    if (s.active) {
      setBusy(true);
      runMode = s.mode;
      haltBtn.textContent = isDirect(s.mode)
        ? '\\u25A0 Halt (keep progress)' : '\\u25A0 Halt & discard run';
      const phase = PHASES[s.phase] || 'running';
      const q = isDirect(s.mode) ? 'queries paused until it finishes'
                                 : 'files.db still queryable';
      status.textContent = 'Maintenance: ' + (LABELS[s.mode] || s.mode)
        + ' — ' + phase + '… (' + q + ')';
    } else {
      setBusy(false);
      clearInterval(polling); polling = null;
      const out_ = isDirect(s.mode)
        ? ((s.exit_code === 0) ? 'finished, changes saved' : 'stopped, progress kept')
        : ((s.exit_code === 0) ? 'committed' : 'discarded');
      status.textContent = 'Maintenance finished — ' + out_
        + ' (exit ' + s.exit_code + ').';
      haltBtn.textContent = '\\u25A0 Halt & discard run';   // reset to default
      loadStats();  // a committed run changes counts and the sync time
    }
  };
  await tick();
  polling = setInterval(tick, 1500);
}

// If a task is already running when the page loads, resume showing it.
fetch('/api/run/status').then(r => r.json()).then(s => {
  if (s.active) { showLog(); poll(); }
});

// ---- Row selection + inspector panel ----
const inspect = document.getElementById('inspect');
const inspectGrid = document.getElementById('inspect-grid');
const inspectTitle = document.getElementById('inspect-title');
const inspectDupes = document.getElementById('inspect-dupes');

function selectRow(tr){
  if (selTr) selTr.classList.remove('sel');
  selTr = tr || null;
  if (!selTr) { inspect.hidden = true; return; }
  selTr.classList.add('sel');
  selTr.scrollIntoView({block: 'nearest'});
  showInspect(selTr);
}

function showInspect(tr){
  const cols = lastData.columns, row = tr._row;
  inspectGrid.innerHTML = '';
  cols.forEach((c, i) => {
    const k = document.createElement('div');
    k.className = 'k'; k.textContent = c;
    const v = document.createElement('div');
    v.className = 'v';
    let val = row[i];
    if (val === null) val = '∅';
    else if (isNum(val) && isByteCol(c))
      val = fmtBytes(val) + '  (' + val.toLocaleString() + ' bytes)';
    v.textContent = val;
    inspectGrid.appendChild(k);
    inspectGrid.appendChild(v);
  });
  inspectTitle.textContent = tr._path || '(no path column in this result)';
  const md5Idx = cols.indexOf('md5');
  const md5 = md5Idx !== -1 && typeof row[md5Idx] === 'string' ? row[md5Idx] : null;
  inspectDupes.hidden = !md5;
  if (md5) inspectDupes.onclick = () => dupList(md5);
  for (const b of inspect.querySelectorAll('[data-act]')) {
    b.disabled = !tr._path;
    b.onclick = () => b.dataset.act === 'copy'
      ? copyPath(tr._path) : fileAction(tr._path, b.dataset.act);
  }
  inspect.hidden = false;
}
document.getElementById('inspect-close').onclick = () => { inspect.hidden = true; };

// ---- Keyboard: arrows move the selection, Enter opens, Space Quick Looks ----
document.addEventListener('keydown', e => {
  const el = document.activeElement;
  if (el && (el.tagName === 'TEXTAREA' || el.tagName === 'INPUT'
             || el.tagName === 'SELECT')) return;
  if (!lastData || out.style.display === 'none') return;
  const rows = [...out.querySelectorAll('table tr')].slice(1)
    .filter(tr => tr.style.display !== 'none');
  if (!rows.length) return;
  if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
    e.preventDefault();
    let i = rows.indexOf(selTr);
    i = (e.key === 'ArrowDown') ? Math.min(i + 1, rows.length - 1) : Math.max(i - 1, 0);
    selectRow(rows[i]);
  } else if (e.key === 'Enter' && selTr && selTr._path) {
    fileAction(selTr._path, 'open');
  } else if (e.key === ' ' && selTr && selTr._path) {
    e.preventDefault();
    fileAction(selTr._path, 'preview');
  } else if ((e.metaKey || e.ctrlKey) && e.key === 'c' && selTr && selTr._path
             && !getSelection().toString()) {
    copyPath(selTr._path);
  }
});

// ---- SQL history (Cmd/Ctrl+Up/Down in the SQL box) ----
let sqlHist = [];
try { sqlHist = JSON.parse(localStorage.getItem('kendexSqlHistory') || '[]'); } catch (e) {}
let histIdx = -1;
function pushHistory(q){
  q = (q || '').trim();
  histIdx = -1;
  if (!q || sqlHist[sqlHist.length - 1] === q) return;
  sqlHist.push(q);
  if (sqlHist.length > 50) sqlHist = sqlHist.slice(-50);
  localStorage.setItem('kendexSqlHistory', JSON.stringify(sqlHist));
}

// ---- User-saved query presets (persisted in localStorage) ----
const userPresetsEl = document.getElementById('user-presets');
let userPresets = [];
try { userPresets = JSON.parse(localStorage.getItem('kendexUserPresets') || '[]'); } catch (e) {}
function persistUserPresets(){
  localStorage.setItem('kendexUserPresets', JSON.stringify(userPresets));
}
function renderUserPresets(){
  userPresetsEl.innerHTML = '';
  userPresets.forEach((pr, i) => {
    const b = document.createElement('button');
    b.textContent = pr.name;
    b.title = pr.q;
    b.onclick = () => { sql.value = pr.q; run(); };
    const del = document.createElement('span');
    del.textContent = '×';
    del.className = 'preset-del';
    del.title = 'Delete this saved query';
    del.onclick = (e) => {
      e.stopPropagation();
      if (!confirm('Delete saved query "' + pr.name + '"?')) return;
      userPresets.splice(i, 1);
      persistUserPresets();
      renderUserPresets();
    };
    b.appendChild(del);
    userPresetsEl.appendChild(b);
  });
}
renderUserPresets();
document.getElementById('savequery').onclick = () => {
  const q = sql.value.trim();
  if (!q) { alert('The SQL box is empty.'); return; }
  const name = (prompt('Name this query:') || '').trim();
  if (!name) return;
  userPresets.push({name: name, q: q});
  persistUserPresets();
  renderUserPresets();
};

// ---- Export the visible (filtered) rows as CSV ----
function csvCell(v){
  if (v === null || v === undefined) return '';
  const s = String(v);
  return /[",\\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
}
exportBtn.onclick = () => {
  if (!lastData) return;
  const q = filterBox.value.trim().toLowerCase();
  const rows = lastData.rows.filter(r => !q ||
    r.map(v => v === null ? '' : String(v)).join(' ').toLowerCase().includes(q));
  const lines = [lastData.columns.map(csvCell).join(',')];
  for (const r of rows) lines.push(r.map(csvCell).join(','));
  const blob = new Blob([lines.join('\\n') + '\\n'], {type: 'text/csv'});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'kendex_results.csv';
  a.click();
  // Deferred: revoking synchronously can cancel the download in Chromium.
  setTimeout(() => URL.revokeObjectURL(a.href), 10000);
};

// ---- Index stats strip, volume datalist, offline set, first-run state ----
const statsEl = document.getElementById('stats');
const volListEl = document.getElementById('vol-list');
async function loadStats(){
  let s;
  try { s = await (await fetch('/api/stats')).json(); } catch (e) { return; }
  document.body.classList.toggle('nodb', !!s.no_db);
  offlineVols = new Set((s.volumes || []).filter(v => !v.mounted).map(v => v.name));
  volListEl.innerHTML = '';
  for (const v of (s.volumes || [])) {
    const o = document.createElement('option');
    o.value = v.name;
    volListEl.appendChild(o);
  }
  statsEl.innerHTML = '';
  const add = (txt, cls) => {
    const sp = document.createElement('span');
    if (cls) sp.className = cls;
    sp.textContent = txt;
    statsEl.appendChild(sp);
  };
  if (s.no_db) { add('no index yet — run Scan for new to create one', 'warn'); return; }
  if (s.error) { add(s.error, 'warn'); return; }
  add(s.files.toLocaleString() + ' files');
  add(fmtBytes(s.bytes) + ' indexed');
  const off = (s.volumes || []).filter(v => !v.mounted).length;
  add((s.volumes || []).length + ' volumes' + (off ? ' (' + off + ' offline)' : ''),
      off ? 'warn' : '');
  if (s.synced_at) {
    const stale = (s.synced_age_days || 0) > 7;
    add('synced ' + (s.synced_age_days < 1 ? 'today' : s.synced_age_days + 'd ago')
        + ' (' + s.synced_at + ')', stale ? 'warn' : '');
  }
}
statsEl.onclick = loadStats;
loadStats();

// ---- Duplicate manager: tick copies to delete, export a reviewed list ----
const dupModal = document.getElementById('dupmodal');
const dupListEl = document.getElementById('dup-list');
const dupSummary = document.getElementById('dup-summary');
const DUP_SQL =
  'WITH d AS (SELECT md5, sum(size_bytes) AS total FROM files ' +
  'WHERE md5 IS NOT NULL GROUP BY md5 HAVING count(*) > 1 ' +
  'ORDER BY total DESC LIMIT 80) ' +
  'SELECT f.md5, f.path, f.size_bytes, f.modified_at ' +
  'FROM files f JOIN d ON f.md5 = d.md5 ' +
  'ORDER BY d.total DESC, f.md5, f.modified_at DESC';
let dupGroups = [];   // [{md5, size, items: [{path, mtime, cb}]}]

document.getElementById('dupes-open').onclick = async () => {
  dupListEl.textContent = 'Loading…';
  dupSummary.textContent = '';
  dupModal.hidden = false;
  let data;
  try {
    const r = await fetch('/api/query', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({sql: DUP_SQL})
    });
    data = await r.json();
  } catch (e) { dupListEl.textContent = 'Request failed: ' + e; return; }
  if (data.error) { dupListEl.textContent = data.error; return; }
  buildDupGroups(data);
};

function buildDupGroups(data){
  const by = new Map();
  for (const [md5, path, size, mtime] of data.rows) {
    if (!by.has(md5)) by.set(md5, {md5: md5, size: size, items: []});
    by.get(md5).items.push({path: path, mtime: mtime || ''});
  }
  dupGroups = [...by.values()].filter(g => g.items.length > 1);
  dupListEl.innerHTML = '';
  if (!dupGroups.length) {
    dupListEl.textContent =
      'No duplicate groups found (md5s are computed during indexing).';
    return;
  }
  for (const g of dupGroups) {
    const head = document.createElement('div');
    head.className = 'dup-head';
    head.textContent = g.items.length + ' copies · ' + fmtBytes(g.size) + ' each · ';
    const w = document.createElement('span');
    w.className = 'waste';
    w.textContent = fmtBytes(g.size * (g.items.length - 1)) + ' reclaimable · md5 '
      + g.md5.slice(0, 10) + '…';
    head.appendChild(w);
    dupListEl.appendChild(head);
    for (const it of g.items) {
      const row = document.createElement('label');
      row.className = 'dup-row';
      const cb = document.createElement('input');
      cb.type = 'checkbox';
      cb.onchange = updateDupSummary;
      it.cb = cb;
      const m = document.createElement('span');
      m.className = 'm';
      m.textContent = (it.mtime || '').slice(0, 10);
      const pspan = document.createElement('span');
      pspan.className = 'p';
      pspan.textContent = it.path;
      row.appendChild(cb);
      row.appendChild(m);
      row.appendChild(pspan);
      dupListEl.appendChild(row);
    }
  }
  updateDupSummary();
}

function updateDupSummary(){
  let n = 0, bytes = 0;
  for (const g of dupGroups)
    for (const it of g.items)
      if (it.cb && it.cb.checked) { n++; bytes += g.size; }
  dupSummary.textContent = n
    ? n + ' copies marked · ' + fmtBytes(bytes) + ' to reclaim'
    : 'nothing marked';
}

document.getElementById('dup-newest').onclick = () => {
  // Rows arrive newest-first within each group (ORDER BY modified_at DESC),
  // so keep item 0 and mark the rest.
  for (const g of dupGroups) g.items.forEach((it, i) => { it.cb.checked = i > 0; });
  updateDupSummary();
};
document.getElementById('dup-clear').onclick = () => {
  for (const g of dupGroups) for (const it of g.items) it.cb.checked = false;
  updateDupSummary();
};
document.getElementById('dup-export').onclick = () => {
  const doomed = [];
  for (const g of dupGroups) {
    const marked = g.items.filter(it => it.cb.checked);
    if (marked.length && marked.length === g.items.length) {
      alert('Every copy of md5 ' + g.md5.slice(0, 10) + '… is marked — at least one '
        + 'copy of each file must stay. Untick one and export again.');
      return;
    }
    doomed.push(...marked.map(it => it.path));
  }
  if (!doomed.length) { alert('Nothing is marked for deletion.'); return; }
  const blob = new Blob([doomed.join('\\n') + '\\n'], {type: 'text/plain'});
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = 'kendex_delete_list.txt';
  a.click();
  // Deferred: revoking synchronously can cancel the download in Chromium.
  setTimeout(() => URL.revokeObjectURL(a.href), 10000);
};
document.getElementById('dup-close').onclick = () => { dupModal.hidden = true; };
document.getElementById('dup-x').onclick = () => { dupModal.hidden = true; };

// ---- Edit exclude list (writes exclude_paths.json; applies on next crawl) ----
const exModal = document.getElementById('exmodal');
const exUser = document.getElementById('ex-user');
const exMsg = document.getElementById('ex-msg');
document.getElementById('edit-excludes').onclick = async () => {
  exMsg.textContent = '';
  let d;
  try { d = await (await fetch('/api/excludes')).json(); }
  catch (e) { alert('Could not load exclude list: ' + e); return; }
  document.getElementById('ex-defaults').textContent = (d.defaults || []).join('\\n');
  exUser.value = (d.user || []).join('\\n');
  exModal.hidden = false;
};
document.getElementById('ex-cancel').onclick = () => { exModal.hidden = true; };
document.getElementById('ex-x').onclick = () => { exModal.hidden = true; };
document.getElementById('ex-save').onclick = async () => {
  const lines = exUser.value.split('\\n').map(s => s.trim()).filter(Boolean);
  const valid = s => (s.startsWith('/') || s.includes('*') || s.includes('?'))
    && [...s].some(c => c !== '*' && c !== '?' && c !== '/');
  const bad = lines.filter(s => !valid(s));
  if (bad.length) {
    alert('Each line must be an absolute path or a glob with at least one '
      + 'real character (a lone * is rejected):\\n' + bad.join('\\n'));
    return;
  }
  exMsg.textContent = 'Saving\\u2026';
  const d = await (await fetch('/api/excludes', {method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({user: lines})})).json();
  if (d.error) { exMsg.textContent = ''; alert(d.error); return; }
  exUser.value = (d.user || []).join('\\n');
  exMsg.textContent = 'Saved \\u2014 applies on the next crawl.';
};
</script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    def _reject_if_unsafe(self, post=False) -> bool:
        """Block cross-origin browser attacks on this localhost server. Returns
        True (and sends 403) if the request must be rejected.

        The browser is a bridge from any web page to 127.0.0.1, and a malicious
        page could otherwise fire side-effecting requests at us (kick off a
        crawl, or run a SELECT that makes DuckDB read local files and smuggle
        them out via a fetched URL). Two cheap defenses:
          * Host must be localhost/127.0.0.1 — defeats DNS rebinding (a hostile
            domain pointed at 127.0.0.1 still sends its own name in Host).
          * POSTs must be application/json — NOT a CORS 'simple' content type, so
            a cross-origin POST must first send an OPTIONS preflight, which we
            never answer; that blocks the no-preflight 'simple' POST trick.
        Our own same-origin fetches don't preflight, so the UI is unaffected."""
        if self.headers.get("Host", "").split(":")[0] not in ("127.0.0.1", "localhost"):
            self._send(403, json.dumps({"error": "forbidden host"}))
            return True
        if post and self.headers.get("Content-Type", "").split(";")[0].strip() != "application/json":
            self._send(403, json.dumps({"error": "Content-Type must be application/json"}))
            return True
        return False

    def _send(self, code, body, ctype="application/json"):
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")  # always serve fresh UI
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self._reject_if_unsafe():
            return
        if self.path.split("?")[0] in ("/", "/index.html"):
            page = (
                PAGE.replace("__PRESETS__", json.dumps(PRESETS))
                .replace("__MAX_ROWS__", str(MAX_ROWS))
            )
            self._send(200, page, "text/html; charset=utf-8")
        elif self.path == "/api/run/status":
            self._send(200, json.dumps(run_status()))
        elif self.path == "/api/stats":
            self._send(200, json.dumps(get_stats()))
        elif self.path == "/api/excludes":
            self._send(200, json.dumps(get_excludes()))
        else:
            self._send(404, json.dumps({"error": "not found"}))

    def do_POST(self):
        if self._reject_if_unsafe(post=True):
            return
        length = int(self.headers.get("Content-Length", 0))
        try:
            payload = json.loads(self.rfile.read(length) or b"{}")
        except Exception:
            self._send(400, json.dumps({"error": "bad request"}))
            return

        if self.path == "/api/query":
            sql = (payload.get("sql") or "").strip()
            if not sql:
                self._send(200, json.dumps({"error": "Empty query"}))
                return
            self._send(200, json.dumps(run_query(sql)))
        elif self.path == "/api/open":
            self._send(200, json.dumps(open_path(
                payload.get("path", ""), payload.get("action", "open"))))
        elif self.path == "/api/run":
            self._send(200, json.dumps(start_run(payload.get("mode", ""))))
        elif self.path == "/api/run/halt":
            self._send(200, json.dumps(halt_run()))
        elif self.path == "/api/excludes":
            self._send(200, json.dumps(save_user_excludes(payload.get("user", []))))
        else:
            self._send(404, json.dumps({"error": "not found"}))

    def log_message(self, *args):  # quiet the default per-request logging
        pass


def main():
    ap = argparse.ArgumentParser(description="Web UI for the file index")
    ap.add_argument("--port", type=int, default=8800)
    ap.add_argument("--host", default="127.0.0.1")
    args = ap.parse_args()
    _open_con()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    url = f"http://{args.host}:{args.port}"
    print(f"Kendex UI → {url}   (DB: {DB_PATH}, read-only)", flush=True)
    print("Ctrl-C to stop.", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nbye")


if __name__ == "__main__":
    main()
