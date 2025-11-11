# core/ts_store.py
from __future__ import annotations
from pathlib import Path
from datetime import datetime, timezone
import json, os

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "storage" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

def _ymd(ts_ms: int | None = None) -> str:
    if ts_ms is None:
        ts_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    return datetime.fromtimestamp(ts_ms/1000, tz=timezone.utc).strftime("%Y%m%d")

def _path(base: str, ts_ms: int | None = None) -> Path:
    return LOG_DIR / f"{base}_{_ymd(ts_ms)}.jsonl"

def append_jsonl(base: str, row: dict, ts_ms: int | None = None):
    """Append a single JSON row to <base>_YYYYMMDD.jsonl."""
    p = _path(base, ts_ms if ts_ms is not None else row.get("t"))
    with p.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, separators=(",", ":")) + "\n")

def read_last_jsonl(base: str, n: int, ts_ms: int | None = None) -> list[dict]:
    """Return last n rows from today's file; if fewer, just all."""
    p = _path(base, ts_ms)
    if not p.exists():
        return []
    # simple tail: read all lines (fast enough for a few MB)
    # if you expect >10MB/day, we can implement a block-seek tail.
    with p.open("r", encoding="utf-8") as f:
        lines = f.readlines()
    tail = lines[-n:]
    out = []
    for ln in tail:
        ln = ln.strip()
        if not ln:
            continue
        try:
            out.append(json.loads(ln))
        except Exception:
            pass
    return out
