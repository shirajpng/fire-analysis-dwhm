#!/usr/bin/env bash
# Extract every dataset for the ITCS544 project, in dependency order.
#
#   cp .env.example .env && $EDITOR .env      # from the repo root
#   data-extraction-scripts/run_all.sh        # from anywhere
#
# Safe to re-run: every step skips work already done.

set -uo pipefail

# Run from this script's directory, so the bare script names below resolve
# no matter where it is invoked from.
cd "$(dirname "${BASH_SOURCE[0]}")" || exit 1

# --- credentials -------------------------------------------------------
# Loaded from .env, which is gitignored. Never hardcode keys in this file.
# See .env.example at the repo root for the template. Checked here first,
# then the repo root, matching where config.py looks.

env_file=""
for candidate in .env ../.env; do
  if [ -f "$candidate" ]; then
    env_file=$candidate
    break
  fi
done

if [ -n "$env_file" ]; then
  set -a
  # shellcheck disable=SC1090
  source "$env_file"
  set +a
  echo "loaded credentials from $env_file"
else
  echo "No .env found. From the repo root run: cp .env.example .env  and fill it in."
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

# A failed step does not stop the run, but it is collected and reported at
# the end, and the script exits non-zero so a failure is never mistaken for
# a clean run.
failed=()

run_step () {
  local label=$1
  echo
  echo "=== $label ==="
  shift
  if ! "$@"; then
    echo "[step failed, continuing]"
    failed+=("$label")
  fi
}

run_step "1/8  Administrative boundaries (fast)"   python extract_boundaries.py --inspect
run_step "2/8  WDPA current release"               python extract_wdpa.py --both
run_step "3/8  NDVI tasks (async)"                 python extract_appeears.py --submit
run_step "4/8  Land cover tasks (async)"           python extract_landcover.py --submit
run_step "5/8  FIRMS NRT (before it rolls off)"    python extract_firms_nrt.py
run_step "6/8  FIRMS country archive (no key)"     python extract_firms_country.py
run_step "7/8  FIRMS archive (the long one)"       python extract_firms.py
run_step "8/8  Inventory"                          python inventory.py

echo
echo "AppEEARS runs server-side. Collect it later with:"
echo "  python extract_appeears.py --status     python extract_landcover.py --status"
echo "  python extract_appeears.py --download   python extract_landcover.py --download"

if [ "${#failed[@]}" -gt 0 ]; then
  echo
  echo "${#failed[@]} step(s) FAILED:"
  printf '  %s\n' "${failed[@]}"
  exit 1
fi
