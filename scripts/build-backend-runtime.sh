#!/usr/bin/env bash
# Build a fully self-contained Python backend runtime under runtime/ so the
# packaged Kendex.app needs no uv, no Homebrew, and no network on the user's
# machine. Run from anywhere; requires uv + Homebrew libmagic on the BUILD host
# only (CI installs both).
#
#   runtime/python/         relocatable standalone CPython (python-build-standalone)
#   runtime/site-packages/  duckdb, python-magic, send2trash, tqdm, pyexiftool (flat)
#   runtime/libmagic/       libmagic.dylib + magic.mgc (signature database)
#
# main.js launches runtime/python/bin/python3.x query_app.py with
# PYTHONPATH=site-packages, MAGIC=libmagic/magic.mgc, and
# DYLD_FALLBACK_LIBRARY_PATH=libmagic so python-magic finds the native lib.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
RUNTIME="$ROOT/runtime"
PYVER="3.14"
UV="${UV:-$(command -v uv || echo /opt/homebrew/bin/uv)}"

echo "==> Clean runtime/"
rm -rf "$RUNTIME"
mkdir -p "$RUNTIME/libmagic"

echo "==> Fetch a relocatable standalone CPython $PYVER (python-build-standalone)"
# --managed-python forces uv's downloaded build, never Homebrew's (which is not
# relocatable and links Homebrew dylibs).
"$UV" python install --managed-python "$PYVER"
PYBIN="$("$UV" python find --managed-python "$PYVER")"
# The executable lives at <install>/bin/pythonX.Y — walk up to the install root.
# pwd -P resolves the cpython-3.14 -> cpython-3.14.4 symlink so we copy real files
# (a symlinked tree would dangle once packaged).
PYINSTALL="$(cd "$(dirname "$PYBIN")/.." && pwd -P)"
echo "    standalone python: $PYINSTALL"

echo "==> Copy interpreter into runtime/python"
cp -R "$PYINSTALL" "$RUNTIME/python"
# Trim weight we never use: the stdlib test suite and bytecode caches.
find "$RUNTIME/python" -type d -name '__pycache__' -prune -exec rm -rf {} + 2>/dev/null || true
rm -rf "$RUNTIME/python/lib/python${PYVER}/test" \
       "$RUNTIME/python/lib/python${PYVER}/idlelib" \
       "$RUNTIME/python/lib/python${PYVER}/ensurepip" 2>/dev/null || true

echo "==> Install deps flat into runtime/site-packages"
# Pin to the locked versions so the bundle matches what we test.
"$UV" export --format requirements-txt --no-hashes --no-emit-project > "$RUNTIME/.req.txt"
"$UV" pip install \
  --python "$RUNTIME/python/bin/python${PYVER}" \
  --target "$RUNTIME/site-packages" \
  -r "$RUNTIME/.req.txt"
rm -f "$RUNTIME/.req.txt"
find "$RUNTIME/site-packages" -type d -name '__pycache__' -prune -exec rm -rf {} + 2>/dev/null || true

echo "==> Bundle libmagic + magic database"
# Resolve the real dylib (portable; macOS readlink -f is unreliable on older OS).
MAGIC_SRC="$(python3 - <<'PY'
import os
print(os.path.realpath('/opt/homebrew/lib/libmagic.dylib'))
PY
)"
cp "$MAGIC_SRC" "$RUNTIME/libmagic/libmagic.dylib"
chmod u+w "$RUNTIME/libmagic/libmagic.dylib"
# No install_name_tool: python-magic dlopens 'libmagic.dylib' by leaf name, which
# DYLD_FALLBACK_LIBRARY_PATH resolves to this copy — the install-id is irrelevant,
# and rewriting it would only invalidate the signature.
cp /opt/homebrew/share/misc/magic.mgc "$RUNTIME/libmagic/magic.mgc"

echo "==> Ad-hoc sign bundled Mach-O (valid signatures for local runs; CI re-signs with Developer ID)"
find "$RUNTIME" -type f \( -name '*.dylib' -o -name '*.so' \) -print0 \
  | while IFS= read -r -d '' f; do codesign --force --sign - "$f" >/dev/null 2>&1 || true; done

echo "==> Smoke test the bundled runtime (no uv, no Homebrew on PATH)"
env -i \
  PYTHONPATH="$RUNTIME/site-packages" \
  MAGIC="$RUNTIME/libmagic/magic.mgc" \
  DYLD_FALLBACK_LIBRARY_PATH="$RUNTIME/libmagic" \
  "$RUNTIME/python/bin/python${PYVER}" - <<'PY'
import duckdb, magic, send2trash, tqdm
print("duckdb", duckdb.__version__)
print("magic mime of this script-ish:", magic.from_buffer(b"%PDF-1.4", mime=True))
print("OK: bundled runtime imports cleanly")
PY

echo "==> Done. Size:"
du -sh "$RUNTIME"
