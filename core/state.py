from __future__ import annotations
from dataclasses import dataclass, asdict, field
from pathlib import Path
import json, time, threading
import numpy as np
from datetime import datetime, timezone
import gzip, shutil  # rotation helpers

ROOT = Path(__file__).resolve().parents[1]

# storage root (DEFINE THIS BEFORE using it anywhere else)
STORAGE = ROOT / "storage"
STORAGE.mkdir(parents=True, exist_ok=True)

STATE_PATH = STORAGE / "state.json"

# --- segmented snapshots (2-minute buckets) ---
SEGMENT_SEC = 120
SEGMENT_MS  = SEGMENT_SEC * 1000
SEG_DIR     = STORAGE / "segments"
SEG_DIR.mkdir(parents=True, exist_ok=True)

# --- 2-minute rolling window (public for UI) ---
WINDOW_SEC = 120
WINDOW_MS  = WINDOW_SEC * 1000

# ---- Size/time knobs ----
TEN_MIN_MS = WINDOW_MS            # legacy alias, now == 2 minutes
SAMPLE_MS  = 1000                 # quantize intraday ts to 1s bins
MAX_ORDERS_IN_STATE  = 200
MAX_METRICS_IN_STATE = 400

# rotate JSONL around this size (compressed)
JSONL_MAX_BYTES = 50_000_000



DEBUG_SEGMENTS = False  # flip to False to silence
def _dbg(*a):
    if DEBUG_SEGMENTS:
        print("[segments]", *a)

def _seg_start_ms(now_ms: int) -> int:
    return (now_ms // SEGMENT_MS) * SEGMENT_MS  # floor-align to 2-min bucket

def _seg_path(start_ms: int) -> Path:
    # Example: storage/segments/state.1731264000000.json
    return SEG_DIR / f"state.{start_ms}.json"

def _list_segment_starts() -> list[int]:
    out = []
    for p in SEG_DIR.glob("state.*.json"):
        try:
            out.append(int(p.stem.split(".")[1]))
        except Exception:
            continue
    return sorted(out)

def _prune_old_segments(keep: int = 2) -> None:
    starts = _list_segment_starts()
    if len(starts) <= keep:
        return
    for s in starts[:-keep]:
        try:
            _dbg("deleting", _seg_path(s).name)
            _seg_path(s).unlink(missing_ok=True)
        except Exception as e:
            _dbg("delete failed", _seg_path(s).name, e)

@dataclass
class SharedState:
    ts_ms: int = 0
    last_prices: dict[str, float] = field(default_factory=dict)
    positions: dict[str, float] = field(default_factory=dict)
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    latency_ms: float = 0.0
    var_90: float = 0.0
    transaction_costs: float = 0.0
    risk: dict = field(default_factory=dict)
    orders: list[dict] = field(default_factory=list)   # ring-buffer of recent acks
    intraday: dict[str, list] = field(default_factory=dict)  # {sym: [{"t": ts_ms, "p": px, "d": "YYYYMMDD"}, ...]}
    metrics: list[dict] = field(default_factory=list)     # last ~600, while full history goes to logs


SEG_EMPTY = {
    "ts_ms": 0,
    "last_prices": {},
    "positions": {},
    "realized_pnl": 0.0,
    "unrealized_pnl": 0.0,
    "latency_ms": 0.0,
    "transaction_costs": 0.0,
    "risk": {},
    "orders": [],
    "intraday": {},  # {sym: [{t,p}, ...]}
    "metrics": [],
}


def _enforce_window(st: "SharedState", window_ms: int = WINDOW_MS) -> None:
    """Trim in-memory arrays to the last window_ms."""
    now_ms = int(time.time() * 1000)
    cutoff = now_ms - window_ms

    # orders: keep by t/ts_ms
    if isinstance(st.orders, list):
        st.orders = [
            o for o in st.orders
            if int(o.get("t") or o.get("ts_ms") or 0) >= cutoff
        ]
        if len(st.orders) > MAX_ORDERS_IN_STATE:
            st.orders = st.orders[-MAX_ORDERS_IN_STATE:]

    # metrics: keep by t
    if isinstance(st.metrics, list):
        st.metrics = [m for m in st.metrics if int(m.get("t", 0)) >= cutoff]
        if len(st.metrics) > MAX_METRICS_IN_STATE:
            st.metrics = st.metrics[-MAX_METRICS_IN_STATE:]

    # intraday: per-symbol {t,p} lists
    if isinstance(st.intraday, dict):
        trimmed = {}
        for sym, bucket in st.intraday.items():
            if not isinstance(bucket, list):
                continue
            b2 = []
            for r in bucket:
                try:
                    t0 = int(r.get("t", 0))
                    p0 = float(r.get("p"))
                except Exception:
                    continue
                if t0 >= cutoff:
                    b2.append({"t": t0, "p": p0})
            if b2:
                trimmed[sym] = b2
        st.intraday = trimmed


class StateStore:
    def __init__(self, path: Path = STATE_PATH):
        self.path = path
        self._lock = threading.Lock()
        if not self.path.exists():
            self._write(SharedState())  # init file

    def read(self) -> SharedState:
        try:
            with self.path.open("r") as f:
                d = json.load(f)
            return SharedState(**d)
        except Exception:
            return SharedState()

    def write(self, st: SharedState):
        with self._lock:
            self._write(st)

    def _write(self, st: SharedState):
        _enforce_window(st, WINDOW_MS)         # <<< enforce trim before every write
        tmp = self.path.with_suffix(".json.tmp")
        d = asdict(st)
        with tmp.open("w") as f:
            json.dump(d, f, separators=(",", ":"), ensure_ascii=False)
        tmp.replace(self.path)


STORE = StateStore()

# ---------- Update helpers ----------
def update_last_price(symbol: str, px: float, latency_ms: float | None = None):
    st = STORE.read()
    st.last_prices[symbol] = px
    st.ts_ms = int(time.time() * 1000)
    if latency_ms is not None:
        st.latency_ms = latency_ms
    STORE.write(st)
    segmented_upsert_snapshot({
        "ts_ms": st.ts_ms,
        "last_prices": st.last_prices,
        "latency_ms": st.latency_ms,
    })

def update_position(symbol: str, qty: float, realized_pnl: float | None = None, cost: float | None = None):
    st = STORE.read()
    st.positions[symbol] = st.positions.get(symbol, 0.0) + qty
    if realized_pnl is not None:
        st.realized_pnl += realized_pnl
    if cost is not None:
        st.transaction_costs += cost
    st.ts_ms = int(time.time() * 1000)
    STORE.write(st)
    segmented_upsert_snapshot({
        "ts_ms": st.ts_ms,
        "positions": st.positions,
        "realized_pnl": st.realized_pnl,
        "transaction_costs": st.transaction_costs,
    })

def set_unrealized(pnl: float):
    st = STORE.read()
    st.unrealized_pnl = pnl
    st.ts_ms = int(time.time() * 1000)
    STORE.write(st)
    segmented_upsert_snapshot({
        "ts_ms": st.ts_ms,
        "unrealized_pnl": st.unrealized_pnl,
    })

# ---------- Risk and VaR ----------
def update_risk_metrics(returns: list[float], confidence: float = 0.90):
    """Compute rolling 90% one-period Value-at-Risk (VaR)."""
    if len(returns) < 10:
        return
    st = STORE.read()
    # percentile of losses (negative returns)
    var = -np.percentile(returns, (1 - confidence) * 100)
    st.var_90 = float(var)
    st.ts_ms = int(time.time() * 1000)
    STORE.write(st)

def record_risk_snapshot(corr: np.ndarray | None = None, vol: np.ndarray | None = None):
    st = STORE.read()
    st.risk = {
        "corr": corr.tolist() if corr is not None else None,
        "vol": vol.tolist() if vol is not None else None,
    }
    st.ts_ms = int(time.time() * 1000)
    STORE.write(st)
    segmented_upsert_snapshot({
        "ts_ms": st.ts_ms,
        "risk": st.risk,
    })

# NEW: append recent order, keep only last N
def append_order(ack) -> None:
    """Append to small in-memory buffer + durable JSONL."""
    st = STORE.read()
    now_ms = int(time.time() * 1000)
    row = {
        "t": now_ms,
        "ts_ms": now_ms,
        "symbol": ack.symbol,
        "side": ack.side,
        "size": float(ack.size),
        "price": float(ack.price) if ack.price is not None else None,
        "venue": ack.venue,
        "status": ack.status,
        "note": ack.note or "",
    }
    append_jsonl("orders", row, now_ms)

    # keep only last 10 minutes in-memory
    cutoff = now_ms - WINDOW_MS
    st.orders.append(row)
    st.orders = [r for r in st.orders if (r.get("t") or r.get("ts_ms", 0)) >= cutoff]
    if len(st.orders) > MAX_ORDERS_IN_STATE:
        st.orders = st.orders[-MAX_ORDERS_IN_STATE:]

    st.ts_ms = now_ms
    STORE.write(st)
    segmented_upsert_snapshot({"orders": [row], "ts_ms": now_ms})




def _q_ms(ms: int, step: int = SAMPLE_MS) -> int:
    """Quantize a millisecond timestamp to a bin (last in bin wins)."""
    if step <= 1:
        return int(ms)
    return int(ms // step * step)

def append_intraday(symbol: str, ts_ms, price: float, max_points_per_day: int = 5000):
    """
    Append a price point for 'symbol' to state, but:
      - keep ONLY last 10 minutes,
      - quantize timestamps to SAMPLE_MS (default 1s),
      - drop legacy 'd' field,
      - skip consecutive duplicates (same price in same bin).
    Also writes a durable JSONL row with rotation.
    """
    # normalize ts to ms
    if isinstance(ts_ms, datetime):
        ts_ms = int(ts_ms.replace(tzinfo=timezone.utc).timestamp() * 1000)
    else:
        ts_ms = int(ts_ms)

    now_ms  = int(time.time() * 1000)
    cutoff  = now_ms - TEN_MIN_MS
    q_ts    = _q_ms(ts_ms, SAMPLE_MS)
    p       = float(price)

    st = STORE.read()
    if not hasattr(st, "intraday") or st.intraday is None:
        st.intraday = {}

    bucket = st.intraday.get(symbol, [])

    # If legacy entries have 'd', drop that field on-the-fly and trim to 10m
    compact = []
    for r in bucket:
        t0 = int(r.get("t", 0))
        if t0 >= cutoff:
            compact.append({"t": t0, "p": float(r.get("p"))})
    bucket = compact

    # dedupe within same bin and consecutive same price
    if bucket:
        last = bucket[-1]
        last_bin = _q_ms(int(last["t"]), SAMPLE_MS)
        last_p   = float(last["p"])
        if last_bin == q_ts and last_p == p:
            # identical bin & price -> skip
            pass
        else:
            bucket.append({"t": q_ts, "p": p})
    else:
        bucket = [{"t": q_ts, "p": p}]

    # strict time trim
    bucket = [r for r in bucket if int(r["t"]) >= cutoff]

    # safety cap by count (rarely hit if time trim works)
    if len(bucket) > max_points_per_day:
        bucket = bucket[-max_points_per_day:]

    st.intraday[symbol] = bucket
    st.ts_ms = now_ms
    STORE.write(st)
    segmented_upsert_snapshot({"intraday": {symbol: [{"t": int(ts_ms), "p": p}]}, "ts_ms": now_ms})

    # durable JSONL (unquantized ts is fine; keep what exchange sent)
    append_jsonl("intraday", {"symbol": symbol, "t": int(ts_ms), "p": p}, int(ts_ms))


def append_metric(ts_ms: int | None = None,
                  realized: float | None = None,
                  unrealized: float | None = None,
                  costs: float | None = None,
                  var90: float | None = None):
    """
    Append one metrics row (P&L, costs, VaR) to daily JSONL; keep only a 10-minute
    tail in state.json for plotting.
    """
    t = int(ts_ms if ts_ms is not None else time.time() * 1000)
    cutoff = t - TEN_MIN_MS

    st = STORE.read()
    row = {
        "t": t,
        "realized": float(st.realized_pnl if realized is None else realized),
        "unrealized": float(st.unrealized_pnl if unrealized is None else unrealized),
        "costs": float((getattr(st, "transaction_costs", 0.0) or 0.0) if costs is None else costs),
        "var90": (float(st.risk.get("VaR90")) if (var90 is None and isinstance(st.risk, dict) and "VaR90" in st.risk)
                  else (float(var90) if var90 is not None else None)),
    }

    # durable JSONL (rotated)
    append_jsonl("metrics", row, t)

    # in-state tail (strict 10-minute window)
    st.metrics.append(row)
    st.metrics = [m for m in st.metrics if int(m.get("t", 0)) >= cutoff]

    # safety cap (rarely hit)
    if len(st.metrics) > MAX_METRICS_IN_STATE:
        st.metrics = st.metrics[-MAX_METRICS_IN_STATE:]

    st.ts_ms = t
    STORE.write(st)
    segmented_upsert_snapshot({"metrics": [row], "ts_ms": t})

def rotate_jsonl(name: str, max_bytes: int = JSONL_MAX_BYTES):
    """Rotate storage/<name>.jsonl when size exceeds max_bytes. Compress old."""
    fn = STORAGE / f"{name}.jsonl"
    if not fn.exists():
        return
    try:
        if fn.stat().st_size < max_bytes:
            return
        ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        rotated = STORAGE / f"{name}.{ts}.jsonl"
        fn.replace(rotated)
        with rotated.open("rb") as src, gzip.open(str(rotated) + ".gz", "wb") as gz:
            shutil.copyfileobj(src, gz)
        rotated.unlink(missing_ok=True)
    except Exception as e:
        print(f"[state] rotate_jsonl error for {name}: {e}")

def append_jsonl(name: str, row: dict, t=None):
    """
    Append one JSON line to storage/<name>.jsonl with timestamp.
    Rotates when file gets large.
    """
    try:
        fn = STORAGE / f"{name}.jsonl"
        if t is None:
            t = int(datetime.now(timezone.utc).timestamp() * 1000)
        payload = dict(row)
        payload["ts_ms"] = int(t)
        with fn.open("a") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        rotate_jsonl(name)
    except Exception as e:
        print(f"[state] append_jsonl error for {name}: {e}")

# =========================
# Public API for UI imports
# =========================
def load_state_for_ui() -> dict:
    """
    Merge scalars from main STORE (state.json) with arrays from segments.
    Arrays: orders, metrics, intraday come from merged segments (prev+current).
    Scalars: realized_pnl, unrealized_pnl, transaction_costs, positions, last_prices, risk, etc. from STORE.
    """
    # base scalars from the main store
    base = asdict(STORE.read())

    # arrays from segments (buffered)
    seg = load_state_merged_segments()

    for k in ("orders", "metrics", "intraday"):
        if isinstance(seg.get(k), list) or isinstance(seg.get(k), dict):
            base[k] = seg[k]

    # keep the most recent timestamp of the two
    try:
        base_ts = int(base.get("ts_ms", 0) or 0)
        seg_ts  = int(seg.get("ts_ms", 0) or 0)
        base["ts_ms"] = max(base_ts, seg_ts)
    except Exception:
        pass

    return base



def load_state() -> dict:
    """
    Return the current state as a plain dict (for the UI).
    """
    st = STORE.read()
    return asdict(st)

def save_state(d: dict) -> None:
    """
    Persist a (possibly trimmed/mutated) state dict back to storage/state.json.
    Only known fields are written; unknown keys are ignored.
    """
    cur = STORE.read()
    # Update fields if present in dict
    if "ts_ms" in d:               cur.ts_ms = int(d["ts_ms"])
    if "last_prices" in d:         cur.last_prices = dict(d["last_prices"])
    if "positions" in d:           cur.positions = dict(d["positions"])
    if "realized_pnl" in d:        cur.realized_pnl = float(d["realized_pnl"])
    if "unrealized_pnl" in d:      cur.unrealized_pnl = float(d["unrealized_pnl"])
    if "latency_ms" in d:          cur.latency_ms = float(d["latency_ms"])
    if "var_90" in d:              cur.var_90 = float(d["var_90"])
    if "transaction_costs" in d:   cur.transaction_costs = float(d["transaction_costs"])
    if "risk" in d:                cur.risk = dict(d["risk"]) if d["risk"] is not None else {}
    if "orders" in d:              cur.orders = list(d["orders"])
    if "intraday" in d:            cur.intraday = dict(d["intraday"])
    if "metrics" in d:             cur.metrics = list(d["metrics"])
    cur.ts_ms = int(time.time() * 1000)
    STORE.write(cur)

def trim_history(H: dict, window_sec: int = WINDOW_SEC) -> None:
    """
    In-place trim of H to the last `window_sec` seconds.
    Trims:
      - H['orders']     by 't' or 'ts_ms'
      - H['metrics']    by 't'
      - H['intraday'][sym] list of {t, p}
    Does nothing if sections are missing.
    """
    now_ms = int(time.time() * 1000)
    cutoff = now_ms - window_sec * 1000

    # orders
    if isinstance(H.get("orders"), list):
        H["orders"] = [
            o for o in H["orders"]
            if int(o.get("t") or o.get("ts_ms") or 0) >= cutoff
        ][:MAX_ORDERS_IN_STATE]

    # metrics
    if isinstance(H.get("metrics"), list):
        H["metrics"] = [
            m for m in H["metrics"]
            if int(m.get("t", 0)) >= cutoff
        ][:MAX_METRICS_IN_STATE]

    # intraday per symbol
    intr = H.get("intraday")
    if isinstance(intr, dict):
        trimmed = {}
        for sym, bucket in intr.items():
            if not isinstance(bucket, list):
                continue
            b2 = [{"t": int(r.get("t")), "p": float(r.get("p"))}
                  for r in bucket
                  if int(r.get("t", 0)) >= cutoff]
            if b2:
                trimmed[sym] = b2
        H["intraday"] = trimmed

def _read_json(path: Path, fallback: dict) -> dict:
    if not path.exists():
        return fallback.copy()
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return fallback.copy()

def _write_json_atomic(path: Path, payload: dict) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"), ensure_ascii=False)
    tmp.replace(path)


def _seg_load(start_ms: int) -> dict:
    """Load one segment (or empty)."""
    return _read_json(_seg_path(start_ms), SEG_EMPTY)

def _seg_save(start_ms: int, seg: dict) -> None:
    _write_json_atomic(_seg_path(start_ms), seg)
    _dbg("saved", _seg_path(start_ms).name, "len:",
         len(seg.get("orders", []) or []),
         len(seg.get("metrics", []) or []),
         sum(len(v or []) for v in (seg.get("intraday") or {}).values()))
    _prune_old_segments(keep=2)  # keep only previous + current

def _seg_trim_inplace(seg: dict, start_ms: int) -> None:
    """
    Trim a segment in place to its own 2-min span [start_ms, start_ms+SEGMENT_MS).
    """
    lo = start_ms
    hi = start_ms + SEGMENT_MS

    # orders
    if isinstance(seg.get("orders"), list):
        seg["orders"] = [
            o for o in seg["orders"]
            if lo <= int(o.get("t") or o.get("ts_ms") or 0) < hi
        ]

    # metrics
    if isinstance(seg.get("metrics"), list):
        seg["metrics"] = [
            m for m in seg["metrics"]
            if lo <= int(m.get("t", 0)) < hi
        ]

    # intraday
    intr = seg.get("intraday") or {}
    trimmed = {}
    for sym, bucket in intr.items():
        b2 = []
        for r in (bucket or []):
            try:
                t0 = int(r.get("t", 0))
                p0 = float(r.get("p"))
            except Exception:
                continue
            if lo <= t0 < hi:
                b2.append({"t": t0, "p": p0})
        if b2:
            trimmed[sym] = b2
    seg["intraday"] = trimmed

def segmented_upsert_snapshot(point_updates: dict) -> None:
    """
    Upsert into the *current* 2-minute segment, and keep only 2 segments total.
    point_updates is a partial dict with any of:
      - snapshot scalars: ts_ms, last_prices, positions, realized_pnl, ...
      - 'orders': [ {...}, ... ] (append)
      - 'metrics': [ {...}, ... ] (append)
      - 'intraday': {sym: [ {t,p}, ... ]} (append)
    """
    now_ms = int(time.time() * 1000)
    start_ms = _seg_start_ms(now_ms)

    seg = _seg_load(start_ms)

    # snapshot scalar fields (overwrite if provided)
    for k in ["ts_ms","last_prices","positions","realized_pnl","unrealized_pnl",
              "latency_ms","transaction_costs","risk"]:
        if k in point_updates:
            seg[k] = point_updates[k]

    # append orders/metrics/intraday
    if "orders" in point_updates and isinstance(point_updates["orders"], list):
        seg.setdefault("orders", [])
        seg["orders"].extend(point_updates["orders"])

    if "metrics" in point_updates and isinstance(point_updates["metrics"], list):
        seg.setdefault("metrics", [])
        seg["metrics"].extend(point_updates["metrics"])

    if "intraday" in point_updates and isinstance(point_updates["intraday"], dict):
        seg.setdefault("intraday", {})
        for sym, bucket in point_updates["intraday"].items():
            if not isinstance(bucket, list): 
                continue
            seg["intraday"].setdefault(sym, [])
            seg["intraday"][sym].extend(bucket)

    # make sure the segment contains only its own 2-min span
    _seg_trim_inplace(seg, start_ms)

    # write, prune older segments
    _seg_save(start_ms, seg)

def load_state_merged_segments() -> dict:
    """
    Read previous+current segments, merge them (previous first, then current overriding snapshot scalars),
    and return a dict. The merged dict may cover up to 4 minutes (buffer), but the UI can still
    filter to last 2 minutes for plotting.
    """
    now_ms = int(time.time() * 1000)
    cur_start = _seg_start_ms(now_ms)
    prev_start = cur_start - SEGMENT_MS

    cur  = _seg_load(cur_start)
    prev = _seg_load(prev_start)

    # Merge snapshot scalars: prefer current if present, else previous
    merged = SEG_EMPTY.copy()
    for k in ["ts_ms","last_prices","positions","realized_pnl","unrealized_pnl",
              "latency_ms","transaction_costs","risk"]:
        merged[k] = cur.get(k, prev.get(k, SEG_EMPTY[k]))

    # concat orders/metrics; intraday per symbol
    merged["orders"]  = (prev.get("orders")  or []) + (cur.get("orders")  or [])
    merged["metrics"] = (prev.get("metrics") or []) + (cur.get("metrics") or [])

    merged_intr = {}
    for src in (prev.get("intraday") or {}, cur.get("intraday") or {}):
        for sym, bucket in src.items():
            merged_intr.setdefault(sym, [])
            merged_intr[sym].extend(bucket or [])
    merged["intraday"] = merged_intr

    return merged






#test
def segmented_upsert_snapshot_at(fake_now_ms: int, point_updates: dict) -> None:
    """
    Test-only helper: same as segmented_upsert_snapshot but uses fake_now_ms
    to choose segment boundaries. DOES NOT change timestamps inside payload,
    only which segment file receives the writes.
    """
    start_ms = _seg_start_ms(fake_now_ms)
    seg = _seg_load(start_ms)

    for k in ["ts_ms","last_prices","positions","realized_pnl","unrealized_pnl",
              "latency_ms","transaction_costs","risk"]:
        if k in point_updates:
            seg[k] = point_updates[k]

    if "orders" in point_updates and isinstance(point_updates["orders"], list):
        seg.setdefault("orders", [])
        seg["orders"].extend(point_updates["orders"])

    if "metrics" in point_updates and isinstance(point_updates["metrics"], list):
        seg.setdefault("metrics", [])
        seg["metrics"].extend(point_updates["metrics"])

    if "intraday" in point_updates and isinstance(point_updates["intraday"], dict):
        seg.setdefault("intraday", {})
        for sym, bucket in point_updates["intraday"].items():
            if not isinstance(bucket, list):
                continue
            seg["intraday"].setdefault(sym, [])
            seg["intraday"][sym].extend(bucket)

    _seg_trim_inplace(seg, start_ms)
    _seg_save(start_ms, seg)
