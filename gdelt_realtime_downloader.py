#!/usr/bin/env python3
"""gdelt_realtime_downloader.py –v0.5(2025‑07‑24)
====================================================
Real‑time watcher for the **GDELT v2** GKG streams.
It polls both of the official *last‑update* files and downloads the newest ZIP
whenever either changes.

* English stream:   `http://data.gdeltproject.org/gdeltv2/lastupdate.txt`
* Multilingual stream: `http://data.gdeltproject.org/gdeltv2/lastupdate-translation.txt`

For Python≥3.7 (no 3.10‑only syntax).
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Optional, Set

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import zipfile

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DEFAULT_INTERVAL = 120  # seconds
ENG_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
TRANS_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate-translation.txt"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def build_session(retries: int) -> requests.Session:
    retry = Retry(total=retries, backoff_factor=1, status_forcelist=(500, 502, 503, 504))
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=4)
    s = requests.Session()
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def safe_filename(url: str) -> str:
    return url.rsplit("/", 1)[-1]


def extract_year(ts_filename: str) -> str:
    # filename looks like 20250724144500.gkg.csv.zip or ...translation.gkg.csv.zip
    return ts_filename[:4]


def download_zip(session: requests.Session, url: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".part")
    with session.get(url, stream=True, timeout=30) as r:
        r.raise_for_status()
        with tmp.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):  # 1MB
                if chunk:
                    f.write(chunk)
    tmp.rename(dest)
    return dest


def extract_zip(zip_path: Path, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(str(zip_path)) as zf:
        zf.extractall(str(out_dir))


# ---------------------------------------------------------------------------
# Core poller
# ---------------------------------------------------------------------------


def poll_once(session: requests.Session, url: str) -> Optional[str]:
    """Return newest .gkg.csv.zip URL (endswith gkg.csv.zip), else None."""
    resp = session.get(url, timeout=15)
    resp.raise_for_status()
    lines = [ln.strip() for ln in resp.text.splitlines() if ln.strip()]
    for line in lines:
        if line.endswith(".gkg.csv.zip"):
            parts = line.split()
            return parts[-1]
    return None  # malformed


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


def run(args: argparse.Namespace) -> None:
    logging.basicConfig(format="[%(asctime)s] %(levelname)s %(message)s",
                        level=logging.DEBUG if args.verbose else logging.INFO,
                        datefmt="%H:%M:%S")

    session = build_session(args.retries)

    # state files per stream
    seen_eng: Set[str] = set()
    seen_trans: Set[str] = set()

    backoff = args.interval

    while True:
        try:
            for kind, url, seen, raw_subdir, csv_subdir in (
                ("ENG", ENG_URL, seen_eng, args.raw_subdir_eng, args.csv_subdir_eng),
                ("TRANS", TRANS_URL, seen_trans, args.raw_subdir_trans, args.csv_subdir_trans),
            ):
                if args.verbose:
                    logging.debug("Polling %s", url)
                zip_link = poll_once(session, url)
                if not zip_link:
                    if args.ignore_malformed:
                        continue
                    else:
                        logging.warning("Unexpected %s content – skip", kind)
                        continue
                name = safe_filename(zip_link)
                if name in seen:
                    continue  # already processed

                year = extract_year(name)
                zip_path = Path(args.output) / year / raw_subdir / name
                if zip_path.exists():
                    logging.info("[%s] ZIP already on disk, skipping download: %s", kind, name)
                else:
                    logging.info("[%s] Downloading %s", kind, name)
                    try:
                        download_zip(session, zip_link, zip_path)
                        logging.info("[%s] Downloaded", kind)
                    except Exception as e:
                        logging.error("[%s] Failed to download %s – %s", kind, name, e)
                        continue  # leave for next poll

                # Extract unless skip
                if not args.skip_extract:
                    try:
                        out_dir = Path(args.output) / year / csv_subdir
                        extract_zip(zip_path, out_dir)
                        logging.info("[%s] Extracted to %s", kind, out_dir)
                    except Exception as e:
                        logging.error("[%s] Extract fail: %s", kind, e)

                seen.add(name)

            time.sleep(args.interval)
            backoff = args.interval  # reset after success
        except (requests.RequestException, zipfile.BadZipFile) as e:
            logging.warning("Network/ZIP error: %s – backing off %.0f s", e, backoff)
            time.sleep(backoff)
            backoff = min(backoff * 2, 1800)
        except KeyboardInterrupt:
            logging.info("Interrupted – exiting.")
            sys.exit(0)
        except Exception as e:
            logging.exception("Unhandled error: %s", e)
            if not args.ignore_malformed:
                sys.exit(1)
            time.sleep(backoff)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def get_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Realtime GDELT GKG downloader (ENG + translations)")
    p.add_argument("-o", "--output", default="data", help="root output folder [data]")
    p.add_argument("--interval", type=int, default=DEFAULT_INTERVAL, help="poll period in seconds [120]")
    p.add_argument("--skip-extract", action="store_true", help="download ZIPs only, do not extract")
    p.add_argument("--raw-subdir-eng",  default="rawdata_en",
               help="subdir for English ZIPs [rawdata_en]")
    p.add_argument("--raw-subdir-trans", default="rawdata_tr",
               help="subdir for translation ZIPs [rawdata_tr]")
    p.add_argument("--csv-subdir-eng",  default="csv_en",
               help="subdir for extracted English CSV [csv_en]")
    p.add_argument("--csv-subdir-trans", default="csv_tr",
               help="subdir for extracted multilingual CSV [csv_tr]")
    p.add_argument("-r", "--retries", type=int, default=3, help="HTTP retries per download [3]")
    p.add_argument("-v", "--verbose", action="store_true", help="DEBUG output + heartbeat")
    p.add_argument("--ignore-malformed", action="store_true", help="skip malformed lastupdate without exit")
    return p.parse_args()


if __name__ == "__main__":
    run(get_args())
