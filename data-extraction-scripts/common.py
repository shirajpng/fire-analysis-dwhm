"""
Shared helpers: HTTP with retries, streaming downloads, and manifest logging.

The manifest is the point of this module. Every file that lands in the raw
zone gets a JSON line recording where it came from, when, how big it was and
its SHA-256. That is your ETL audit trail, and the progress report asks for
exactly this kind of provenance.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

import config

# ---------------------------------------------------------------- logging

def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter("%(asctime)s  %(levelname)-7s %(name)-18s %(message)s",
                              datefmt="%H:%M:%S")
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


log = get_logger("common")

# ---------------------------------------------------------------- http

def session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "ITCS544-student-project/1.0"})
    return s


def get_with_retry(
    url: str,
    sess: requests.Session | None = None,
    *,
    timeout: int = 180,
    stream: bool = False,
    **kwargs,
) -> requests.Response:
    """
    GET with exponential backoff. Raises on final failure.

    Client errors (4xx) are NOT retried, with the exception of 429. A 404 means
    the resource is not there and will still not be there in eight seconds;
    retrying it just burns 15 seconds per miss.
    """
    sess = sess or session()
    last: Exception | None = None

    for attempt in range(config.MAX_RETRIES):
        try:
            resp = sess.get(url, timeout=timeout, stream=stream, **kwargs)
            resp.raise_for_status()
            return resp
        except requests.HTTPError as exc:
            code = exc.response.status_code if exc.response is not None else None
            if code and 400 <= code < 500 and code != 429:
                raise  # permanent; let the caller decide
            last = exc
        except Exception as exc:
            last = exc

        wait = 2 ** attempt
        log.warning("retry %d/%d for %s -- %s; sleeping %ds",
                    attempt + 1, config.MAX_RETRIES, url[:80], last, wait)
        time.sleep(wait)

    raise RuntimeError(f"exhausted retries for {url}") from last


def download(url: str, target: Path, *, sess: requests.Session | None = None,
             skip_if_exists: bool = True) -> Path:
    """
    Stream a (possibly large) file to disk. Writes to a .part file first and
    renames on success, so an interrupted run never leaves a truncated file
    that looks complete to the next run.
    """
    target.parent.mkdir(parents=True, exist_ok=True)

    if skip_if_exists and target.exists() and target.stat().st_size > 0:
        log.info("skip (exists): %s", target.name)
        return target

    part = target.with_suffix(target.suffix + ".part")
    resp = get_with_retry(url, sess, stream=True)

    total = int(resp.headers.get("content-length", 0))
    written = 0
    with open(part, "wb") as fh:
        for chunk in resp.iter_content(1 << 20):
            fh.write(chunk)
            written += len(chunk)
            if total:
                pct = 100 * written / total
                print(f"\r  {target.name}: {written >> 20:,} / {total >> 20:,} MiB "
                      f"({pct:5.1f}%)", end="", flush=True)
    if total:
        print()

    part.rename(target)
    log.info("downloaded %s (%.1f MiB)", target.name, written / (1 << 20))

    record_manifest(target, source_url=resp.url, bytes_written=written)
    return target


# ---------------------------------------------------------------- manifest

def sha256(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while block := fh.read(chunk):
            h.update(block)
    return h.hexdigest()


def record_manifest(path: Path, *, source_url: str, bytes_written: int | None = None,
                    rows: int | None = None, extra: dict | None = None) -> None:
    """Append one provenance record. This feeds your ETL audit dimension."""
    config.MANIFEST.parent.mkdir(parents=True, exist_ok=True)

    entry = {
        "path": str(path),
        "source_url": source_url,
        "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
        "bytes": bytes_written if bytes_written is not None else path.stat().st_size,
        "sha256": sha256(path),
    }
    if rows is not None:
        entry["rows"] = rows
    if extra:
        entry.update(extra)

    with open(config.MANIFEST, "a") as fh:
        fh.write(json.dumps(entry) + "\n")
