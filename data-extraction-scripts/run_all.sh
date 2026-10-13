#!/usr/bin/env bash
# Extract every dataset for the ITCS544 project, in dependency order.
#
#   cp .env.example .env && $EDITOR .env
#   ./run_all.sh
#
# Safe to re-run: every step skips work already done.

set -uo pipefail

# --- credentials -------------------------------------------------------
# Loaded from .env, which is gitignored. Never hardcode keys in this file.
# See .env.example for the template.

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
  echo "loaded credentials from .env"
elif [ -f .env.example ]; then
  echo "No .env found. Run: cp .env.example .env  and fill it in."
fi

# --- preflight ---------------------------------------------------------
# Check credentials BEFORE downloading 1.7 GB. Previously a missing key was
# only discovered after the WDPA global download had already completed.

missing=0
for var in FIRMS_MAP_KEY EARTHDATA_USER EARTHDATA_PASS; do
  if [ -z "${!var:-}" ]; then
    echo "MISSING: $var"
    missing=1
  fi
done

if [ "$missing" -eq 1 ]; then
  cat <<'MSG'

  FIRMS_MAP_KEY   https://firms.modaps.eosdis.nasa.gov/api/  (email, minutes)
  EARTHDATA_*     https://urs.earthdata.nasa.gov/            (register, then
                  accept the AppEEARS EULA once before the API will accept you)

Open-access steps (boundaries, WDPA) will still run. Press Ctrl-C to stop.
MSG
  sleep 5
fi

run_step () {
  echo
  echo "=== $1 ==="
  shift
  "$@" || echo "[step failed, continuing]"
}

run_step "1/5  Administrative boundaries (fast)"   python extract_boundaries.py --inspect
run_step "2/5  WDPA current release"               python extract_wdpa.py --both
run_step "3/5  NDVI tasks (async)"                 python extract_appeears.py --submit
run_step "4/5  FIRMS archive (the long one)"       python extract_firms.py
run_step "5/5  Inventory"                          python inventory.py

echo
echo "AppEEARS runs server-side. Collect it later with:"
echo "  python extract_appeears.py --status"
echo "  python extract_appeears.py --download"
