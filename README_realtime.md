# GDELT GKG Realtime Downloader

**Version 0.5 – 24 Jul 2025**  
Watches both GDELT *last‑update* feeds (English + Multilingual) every `--interval`
seconds (default 120 s) and saves each new ZIP into dedicated folders before
(optionally) extracting the CSVs into matching sub‑folders.

---

## Folder Layout (defaults)

| Stream | ZIP Folder | CSV Folder |
|--------|------------|------------|
| English (`lastupdate.txt`) | `rawdata_en/` | `csv_en/` |
| Multi‑language (`lastupdate-translation.txt`) | `rawdata_tr/` | `csv_tr/` |

Change these names with `--raw-subdir-eng`, `--raw-subdir-trans`,
`--csv-subdir-eng`, `--csv-subdir-trans`.

---

## Python 3.7.4 Compatibility

The script avoids the `A | B` union‑type syntax; it runs unmodified on
Python 3.7.4. `typing_extensions` (see *requirements.txt*) back‑ports modern
typing features when running on Python < 3.10.

---

## Installation

```bash
git clone https://github.com/<your-user>/gdelt-realtime-downloader.git
cd gdelt-realtime-downloader
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements_realtime.txt
```

---

## Usage Examples

### 1 — Download‑only, 2‑minute polling, debug heartbeat

```bash
python gdelt_realtime_downloader.py --output data --skip-extract \
       --interval 120 -v
```

### 2 — Custom folder names & auto‑extract

```bash
python gdelt_realtime_downloader.py --output data \
       --raw-subdir-eng rawdata_en --raw-subdir-trans rawdata_tr \
       --csv-subdir-eng csv_en --csv-subdir-trans csv_tr
```

### 3 — Run in background (nohup)

```bash
nohup python gdelt_realtime_downloader.py --output data --skip-extract -v \
      > realtime.log 2>&1 &
```

Stop all realtime processes:

```bash
pkill -f gdelt_realtime_downloader.py          # macOS / Linux
```

---

## Command‑line Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-o, --output DIR` | `data` | Root output folder |
| `--interval SECS` | `120` | Poll period |
| `--skip-extract` | Off | Keep ZIPs only |
| `--raw-subdir-eng` | `rawdata_en` | English ZIP sub‑folder |
| `--raw-subdir-trans` | `rawdata_tr` | Translation ZIP sub‑folder |
| `--csv-subdir-eng` | `csv_en` | English CSV sub‑folder |
| `--csv-subdir-trans` | `csv_tr` | Translation CSV sub‑folder |
| `-v, --verbose` | Off | DEBUG logs + heartbeat |
| `--ignore-malformed` | Off | Skip malformed `lastupdate*.txt` without back‑off |

---

## Requirements

```
requests>=2.31.0
typing_extensions>=4.0; python_version < "3.10"
```

`tqdm` is **not** required for the realtime script.

---

## License

MIT License © 2025 <Your Name>
