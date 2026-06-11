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
# A tqdm progress line, e.g. "Verifying:  40%|████  | 1112231/2762976 [..file/s]".
# Matched by the "NN%|" percentage-bar signature so it's caught whether tqdm
# overwrites with \r (tty-ish) or prints a fresh \n line per refresh (file).
_TQDM_RE = re.compile(r"\d{1,3}%\|")


def get_excludes() -> dict:
    """Built-in (locked) excludes + the user-editable list, for the editor UI."""
    return {
        "defaults": sorted(crawler.EXCLUDE_DEFAULTS),
        "user": sorted(crawler.load_user_excludes()),
    }


def save_user_excludes(paths) -> dict:
    """Write the user exclude list to crawler.EXCLUDE_CONFIG. Only absolute-path
    strings are kept; built-in defaults are never touched. Returns the saved set
    (or an error dict). This writes a plain JSON file of path prefixes — it does
    not feed the fixed crawler COMMANDS, so the run path stays request-input-free."""
    if not isinstance(paths, list):
        return {"error": "expected a list of paths"}
    clean = sorted({p.strip() for p in paths
                    if isinstance(p, str) and p.strip().startswith("/")})
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
COMMANDS = {
    "reindex": _cmd("--reindex-changed"),
    "scan":    _cmd(),
    "prune":   _cmd("--prune"),
    "sync":    " && ".join([_cmd("--reindex-changed"), _cmd(), _cmd("--prune")]),
    "compact": (PY + " compact_db.py "
                + shlex.quote(DB_PATH) + " " + shlex.quote(WORK_DB)),
}

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


PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>File Index</title>
<style>
  :root {
    color-scheme: dark;
    --bg: #0f1115;
    --panel: #151922;
    --panel-2: #1b202a;
    --line: #2b3240;
    --line-soft: #242a35;
    --text: #e7eaf0;
    --muted: #8f98a8;
    --blue: #69a7ff;
    --green: #49c58f;
    --yellow: #e7c75a;
    --red: #ee6678;
    --field: #10131a;
    --hover: #222936;
    --shadow: 0 18px 50px rgba(0,0,0,.26);
  }
  * { box-sizing: border-box; }
  body {
    font: 14px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    margin: 0; display: flex; height: 100vh; color: var(--text);
    background: var(--bg);
  }
  button, input, select, textarea { font: inherit; }
  button { color: inherit; }
  #side {
    width: 250px; flex: none; border-right: 1px solid var(--line);
    padding: 16px 14px; overflow-y: auto; background: var(--panel);
  }
  .brand { display: flex; gap: 10px; align-items: center; margin: 0 0 18px; }
  .brand-mark {
    width: 34px; height: 34px; border-radius: 8px; display: grid; place-items: center;
    background: linear-gradient(135deg, #6ca8ff, #49c58f); color: #081018;
    font-weight: 800; letter-spacing: 0; box-shadow: 0 8px 22px rgba(73,197,143,.18);
  }
  .brand-name { font-size: 15px; font-weight: 700; }
  .brand-meta { font-size: 12px; color: var(--muted); margin-top: 1px; }
  #side h3 {
    margin: 18px 0 8px; font-size: 11px; text-transform: uppercase;
    letter-spacing: .08em; color: var(--muted); font-weight: 700;
  }
  #side h3:first-of-type { margin-top: 0; }
  #side button {
    display: block; width: 100%; text-align: left; margin: 0 0 6px;
    min-height: 34px; padding: 7px 10px; border: 1px solid var(--line-soft);
    border-radius: 7px; background: transparent; cursor: pointer;
    transition: background .12s ease, border-color .12s ease, transform .08s ease;
  }
  #side button:hover { background: var(--hover); border-color: #3a4352; }
  #side button:active { transform: translateY(1px); }
  #presets button { border-left: 3px solid var(--blue); background: rgba(105,167,255,.08); }
  #maint button { border-left: 3px solid var(--green); background: rgba(73,197,143,.07); }
  button#clearlog { border-left: 3px solid var(--yellow); background: rgba(231,199,90,.08); }
  button#edit-excludes { border-left: 3px solid var(--blue); background: rgba(105,167,255,.08); margin-top: 8px; }
  button#halt { border-left: 3px solid var(--red); color: #ff93a0; margin-top: 8px; }
  #side button#halt:hover { background: rgba(238,102,120,.1); border-color: rgba(238,102,120,.45); }
  #side hr { border: none; border-top: 1px solid var(--line); margin: 16px 0; }
  #main {
    flex: 1; display: flex; flex-direction: column; padding: 18px;
    min-width: 0; background: var(--bg);
  }
  textarea {
    width: 100%; height: 118px; padding: 12px; border: 1px solid var(--line);
    border-radius: 8px; background: var(--field); color: var(--text);
    resize: vertical; font: 13px/1.55 ui-monospace, SFMono-Regular, Menlo, monospace;
    outline: none;
  }
  textarea:focus, input:focus, select:focus {
    border-color: rgba(105,167,255,.75); box-shadow: 0 0 0 3px rgba(105,167,255,.14);
  }
  #bar { margin: 10px 0 8px; display: flex; gap: 10px; align-items: center; }
  #run, #locate button {
    min-height: 34px; padding: 6px 14px; border-radius: 7px;
    border: 1px solid rgba(105,167,255,.5); background: rgba(105,167,255,.16);
    cursor: pointer; font-weight: 650;
  }
  #run:hover, #locate button:hover { background: rgba(105,167,255,.23); }
  #status { color: var(--muted); }
  #run-progress {
    height: 7px; border: 1px solid var(--line-soft); border-radius: 999px;
    overflow: hidden; margin: 0 0 10px; display: none; background: #0b0d12; flex: none;
  }
  #run-progress > div { height: 100%; width: 0%; background: var(--green);
                        transition: width .2s ease; }
  #run-progress.indeterminate > div { width: 35%;
                                      animation: progress-sweep 1.1s ease-in-out infinite; }
  @keyframes progress-sweep {
    0% { transform: translateX(-110%); }
    100% { transform: translateX(300%); }
  }
  #out {
    flex: 1; overflow: auto; border: 1px solid var(--line); border-radius: 8px;
    background: #0d0f14; box-shadow: var(--shadow);
  }
  table { border-collapse: collapse; width: 100%; }
  th, td { border-bottom: 1px solid var(--line-soft); padding: 7px 10px; text-align: left;
           white-space: nowrap; max-width: 480px; overflow: hidden;
           text-overflow: ellipsis; }
  th {
    position: sticky; top: 0; background: #171b24; color: var(--muted);
    font-size: 12px; font-weight: 700; z-index: 1;
  }
  td { font: 13px/1.45 ui-monospace, SFMono-Regular, Menlo, monospace; color: #d6dce7; }
  #out tr:hover td { background: rgba(105,167,255,.09); }
  #pathbox {
    border: 1px solid var(--line); border-radius: 8px; padding: 8px 10px;
             margin: 0 0 10px; white-space: pre-wrap; word-break: break-all;
             color: #c7cfdd; background: var(--field);
             font: 13px/1.45 ui-monospace, SFMono-Regular, Menlo, monospace;
             /* 2 text lines + padding + border (border-box): no wrap jiggle */
             min-height: calc(2.9em + 18px); flex: none; }
  #pathbox:empty::before { content: "(hover a result row to see its full path here)";
                           color: var(--muted); }
  td.num { text-align: right; }
  .err { color: #ff93a0; padding: 14px; white-space: pre-wrap; }
  #locate {
    border: 1px solid var(--line); border-radius: 8px; margin: 0 0 10px;
    padding: 12px; display: flex; flex-wrap: wrap; gap: 8px; align-items: center;
    background: var(--panel-2);
  }
  #locate legend {
    font-size: 11px; text-transform: uppercase; letter-spacing: .08em;
    color: var(--muted); padding: 0 6px; font-weight: 700;
  }
  #locate input, #locate select {
    padding: 6px 9px; border: 1px solid var(--line); border-radius: 7px;
    background: var(--field); color: var(--text); outline: none;
    min-height: 34px;
  }
  #locate input[type="checkbox"] { min-height: 0; accent-color: var(--blue); }
  #locate label { display: flex; gap: 6px; align-items: center; color: #cbd2df; }
  .utc-note { font-size: 12px; color: var(--muted); align-self: center; }
  #exmodal { position: fixed; inset: 0; background: rgba(0,0,0,.58); z-index: 50;
             display: flex; align-items: center; justify-content: center; }
  #exmodal[hidden] { display: none; }
  #exmodal-panel {
    background: var(--panel); color: var(--text); border: 1px solid var(--line);
    border-radius: 8px; padding: 18px; width: 560px; max-width: 92vw;
    max-height: 86vh; overflow: auto; box-shadow: var(--shadow);
  }
  #exmodal-panel h3 { margin: 0 0 6px; }
  #exmodal .ex-note { color: var(--muted); font-size: 13px; margin: 0 0 10px; }
  #ex-defaults {
    background: var(--field); border: 1px solid var(--line-soft); border-radius: 7px;
    padding: 8px; color: #cbd2df; max-height: 28vh; overflow: auto;
    white-space: pre-wrap; margin: 4px 0 12px; word-break: break-all;
  }
  #ex-user { width: 100%; height: 150px; box-sizing: border-box; }
  #exmodal .ex-btns { margin-top: 12px; display: flex; gap: 8px; align-items: center; }
  #exmodal .ex-btns button {
    padding: 6px 14px; border-radius: 7px; cursor: pointer;
    border: 1px solid var(--line); background: var(--field);
  }
  #ex-save { border-color: rgba(73,197,143,.55); color: #8be0bb; }
  #ex-msg { color: var(--muted); font-size: 13px; }
  #log {
    flex: 1; overflow: auto; border: 1px solid var(--line); border-radius: 8px;
    padding: 12px; white-space: pre-wrap; display: none; background: #0d0f14;
    font: 13px/1.5 ui-monospace, SFMono-Regular, Menlo, monospace;
  }
  button:disabled { opacity: .4; cursor: not-allowed; }
</style>
</head>
<body>
<div id="side">
  <div class="brand">
    <div class="brand-mark">FI</div>
    <div>
      <div class="brand-name">File Indexer</div>
      <div class="brand-meta">Local desktop index</div>
    </div>
  </div>
  <h3>Presets</h3>
  <div id="presets"></div>
  <hr>
  <h3>Maintenance</h3>
  <div id="maint">
    <button data-mode="reindex">Reindex changed</button>
    <button data-mode="scan">Scan for new</button>
    <button data-mode="prune">Prune deleted</button>
    <button data-mode="sync">Full sync (all 3)</button>
    <button data-mode="compact">Compact DB</button>
  </div>
  <button id="edit-excludes">Edit exclude list</button>
  <button id="clearlog">Clear log window</button>
  <hr>
  <h3>Stop</h3>
  <button id="halt" disabled>&#9632; Halt &amp; discard run</button>
</div>
<div id="main">
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
    <input id="loc-vol" size="18" placeholder="TB5_DOCK8, OWC HD1">
    <button id="loc-go">Locate &#9654;</button>
  </fieldset>
  <textarea id="sql" spellcheck="false"
    placeholder="SELECT * FROM files LIMIT 100"></textarea>
  <div id="bar">
    <button id="run">Run &#9654; (Ctrl+Enter)</button>
    <span id="status"></span>
  </div>
  <div id="run-progress" aria-hidden="true"><div></div></div>
  <div id="pathbox"></div>
  <div id="out"></div>
  <pre id="log"></pre>
</div>
<div id="exmodal" hidden>
  <div id="exmodal-panel">
    <h3>Crawler exclude list</h3>
    <p class="ex-note">Files under these path prefixes are skipped by the crawler.
      Changes take effect on the <b>next</b> crawl, not retroactively.</p>
    <div><b>Built-in (always excluded)</b></div>
    <pre id="ex-defaults"></pre>
    <div><b>Your excludes</b> &mdash; one absolute path per line:</div>
    <textarea id="ex-user" spellcheck="false"
      placeholder="/Volumes/SomeVolume&#10;/Volumes/Other/subdir"></textarea>
    <div class="ex-btns">
      <button id="ex-save">Save</button>
      <button id="ex-cancel">Cancel</button>
      <span id="ex-msg"></span>
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

const presetsEl = document.getElementById('presets');
for (const [name, q] of PRESETS) {
  const b = document.createElement('button');
  b.textContent = name;
  b.onclick = () => { sql.value = q; run(); };
  presetsEl.appendChild(b);
}

function isNum(v){ return typeof v === 'number'; }

let lastData = null;          // most recent result set, for re-sorting in place
let sortCol = -1, sortDir = 0;   // dir: -1 desc, +1 asc, 0 unsorted

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
  // If the result carries an md5 column (e.g. the Duplicate-files preset),
  // clicking a row offers to download the full path list of that md5's copies.
  const md5Idx = data.columns.indexOf('md5');
  for (const row of data.rows) {
    const tr = document.createElement('tr');
    // Show the row's path in the readout pane on hover. Works for any column
    // holding a path (path, example, etc.): first cell that looks like one.
    const p = row.find(v => typeof v === 'string' && v.startsWith('/'));
    if (p !== undefined) {
      tr.onmouseenter = () => { pathbox.textContent = p; };
    }
    if (md5Idx !== -1 && typeof row[md5Idx] === 'string') {
      tr.style.cursor = 'pointer';
      tr.title = 'Click to download the full path list of all copies of this file';
      tr.onclick = () => dupList(row[md5Idx]);
    }
    for (const v of row) {
      const td = document.createElement('td');
      if (isNum(v)) td.className = 'num';
      td.textContent = v === null ? '∅' : v;
      tr.appendChild(td);
    }
    t.appendChild(tr);
  }
  out.appendChild(t);
  status.textContent = data.rows.length + ' rows' +
    (data.truncated ? ' (capped at ' + __MAX_ROWS__ + ')' : '');
}

async function run() {
  status.textContent = 'Running…';
  out.innerHTML = '';
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
  URL.revokeObjectURL(a.href);
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

// ---- Maintenance (runs crawler.py against a disposable copy of files.db) ----
const log = document.getElementById('log');
const maintBtns = [...document.querySelectorAll('#maint button')];
const haltBtn = document.getElementById('halt');
const clearBtn = document.getElementById('clearlog');
const LABELS = {reindex:'Reindex changed', scan:'Scan for new',
                prune:'Prune deleted', sync:'Full sync', compact:'Compact DB'};
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
    if (!confirm('Run "' + LABELS[b.dataset.mode] + '"?\\n\\nThe task works on '
        + 'a copy of files.db; your current DB is only replaced if it finishes. '
        + 'You can Halt & discard at any time.')) return;
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
  if (!confirm('Halt the running scan and discard its in-progress work?\\n\\n'
      + 'Your current files.db is left completely unchanged.')) return;
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
  // The SQL Run button stays enabled — queries work against the live DB during
  // a run (the crawler is writing to a separate copy).
}

function updateProgress(active, progressLine) {
  if (!active) {
    runProgress.style.display = 'none';
    runProgress.classList.remove('indeterminate');
    runProgressBar.style.width = '0%';
    return;
  }
  runProgress.style.display = 'block';
  const m = (progressLine || '').match(/(\\d{1,3})%\\|/);
  if (m) {
    const pct = Math.max(0, Math.min(100, Number(m[1])));
    runProgress.classList.remove('indeterminate');
    runProgressBar.style.width = pct + '%';
  } else {
    runProgress.classList.add('indeterminate');
    runProgressBar.style.width = '';
  }
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
      const phase = PHASES[s.phase] || 'running';
      status.textContent = 'Maintenance: ' + (LABELS[s.mode] || s.mode)
        + ' — ' + phase + '… (files.db still queryable)';
    } else {
      setBusy(false);
      clearInterval(polling); polling = null;
      const out_ = (s.exit_code === 0) ? 'committed' : 'discarded';
      status.textContent = 'Maintenance finished — ' + out_
        + ' (exit ' + s.exit_code + ').';
    }
  };
  await tick();
  polling = setInterval(tick, 1500);
}

// If a task is already running when the page loads, resume showing it.
fetch('/api/run/status').then(r => r.json()).then(s => {
  if (s.active) { showLog(); poll(); }
});

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
document.getElementById('ex-save').onclick = async () => {
  const lines = exUser.value.split('\\n').map(s => s.trim()).filter(Boolean);
  const bad = lines.filter(s => !s.startsWith('/'));
  if (bad.length) { alert('Not absolute paths:\\n' + bad.join('\\n')); return; }
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
    print(f"File index UI → {url}   (DB: {DB_PATH}, read-only)", flush=True)
    print("Ctrl-C to stop.", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nbye")


if __name__ == "__main__":
    main()
