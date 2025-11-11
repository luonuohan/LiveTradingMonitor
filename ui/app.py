# ui/app.py
from __future__ import annotations
from pathlib import Path
from datetime import datetime, timezone
import json, time
import streamlit as st
import pandas as pd
import altair as alt

# === use the state utilities from core.state (do NOT redefine in this file) ===

from core.state import load_state_for_ui as load_state, WINDOW_SEC


# ---------- page ----------
st.set_page_config(page_title="Trading Support — Live Monitor", layout="wide")
st.title("Trading Support — Live Monitor")

# ---------- helpers ----------
def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def fmt_ts(ts_ms: int) -> str:
    if not ts_ms:
        return "—"
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "Z"

def normalize_intraday(rows: list[dict]) -> list[dict]:
    """
    Backward-compatible sanitizer:
      - Accepts legacy rows that may contain 'd'
      - Keeps only fields {t, p}
      - Drops malformed rows
      - Collapses consecutive duplicates
    """
    out = []
    last_t = last_p = None
    for r in rows or []:
        try:
            t = int(r.get("t", 0))
            p = float(r.get("p", r.get("price")))
        except Exception:
            continue
        if not t:
            continue
        if last_t == t and last_p == p:
            continue
        out.append({"t": t, "p": p})
        last_t, last_p = t, p
    return out

def _parse_costs(note: str) -> dict:
    """Parse executor note like 'fee=0.12,½spr=0.08,slip=0.10,cost=0.30' into a dict."""
    out = {}
    if not note:
        return out
    for part in note.split(","):
        if "=" in part:
            k, v = part.split("=", 1)
            k = k.strip()
            v = v.strip()
            try:
                out[k] = float(v)
            except:
                out[k] = v
    return out


# =========================================================
# Load state and compute a moving 2-minute cutoff
# =========================================================
state = load_state()  # from core.state
cutoff_ms = _now_ms() - WINDOW_SEC * 1000  # WINDOW_SEC == 120 by default




risk  = state.get("risk", {}) if isinstance(state, dict) else {}
last_prices = state.get("last_prices", {}) or {}
symbols = sorted(last_prices.keys()) or ["BTC-USD"]

# remember last chosen symbol
if "symbol" not in st.session_state:
    st.session_state.symbol = symbols[0]
st.session_state.symbol = st.selectbox(
    "Symbol",
    symbols,
    index=max(0, symbols.index(st.session_state.symbol) if st.session_state.symbol in symbols else 0),
)
sel = st.session_state.symbol

last_px = float(last_prices.get(sel, 0.0))
lat_ms  = int(state.get("latency_ms", 0) or 0)
ts_ms   = int(state.get("ts_ms", 0) or 0)
ts_str  = fmt_ts(ts_ms)
var90   = risk.get("VaR90", None)

# ---------- top KPIs ----------
k1, k2, k3, k4 = st.columns(4)
k1.metric("Realized P&L", f"{float(state.get('realized_pnl', 0.0)):.2f}")
k2.metric("Unrealized P&L", f"{float(state.get('unrealized_pnl', 0.0)):.2f}")
k3.metric("90% VaR (proxy)", f"{var90:,.2f}" if isinstance(var90, (int, float)) else "n/a")
k4.metric("Txn Costs (cum)", f"{float(state.get('transaction_costs',0.0)):.2f}")

# ---------- price block ----------
left, right = st.columns([1, 2])
with left:
    st.subheader("Last Price")
    st.metric(sel, f"{last_px:,.2f}", help=f"Last update: {ts_str}\nLatency: {lat_ms} ms")
    st.caption(f"Latency: {lat_ms} ms • {ts_str}")

# ---------- Intraday Sparkline (last 2 minutes hard limit) ----------
intraday_raw = (state.get("intraday") or {}).get(sel, [])
rows_all = normalize_intraday(intraday_raw)
rows = [r for r in rows_all if int(r.get("t", 0)) >= cutoff_ms]

right.subheader("Intraday Sparkline")
if rows:
    df = pd.DataFrame(rows)
    df["ts"] = pd.to_datetime(df["t"], unit="ms", utc=True)
    df = df[["ts", "p"]].rename(columns={"p": "price"}).sort_values("ts")

    px_min, px_max = float(df["price"].min()), float(df["price"].max())
    pad = (px_max - px_min) * 0.02 or 1.0
    y_min, y_max = px_min - pad, px_max + pad

    chart = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x=alt.X("ts:T", title="Time (UTC)"),
            y=alt.Y("price:Q", title="", scale=alt.Scale(domain=[y_min, y_max])),
            tooltip=[alt.Tooltip("ts:T", title="Time (UTC)"),
                     alt.Tooltip("price:Q", title="Price", format=",.2f")]
        )
        .properties(height=180, width="container")
    )
    st.altair_chart(chart, width="stretch")
else:
    st.caption("No intraday points in the last 2 minutes…")

# ---------- positions ----------
st.subheader("Positions")
pos = state.get("positions", {}) or {}
if pos:
    rows = []
    for s, q in sorted(pos.items()):
        px = float(last_prices.get(s, 0.0))
        rows.append({"Symbol": s, "Qty": float(q), "Notional (USD)": float(q) * px})
    dfp = pd.DataFrame(rows)[["Symbol", "Qty", "Notional (USD)"]]
    st.dataframe(dfp, width="stretch", height=180)
else:
    st.caption("No open positions yet.")

# ---------- Risk summary tiles ----------
if risk:
    st.subheader("Risk (live)")
    r1, r2, r3 = st.columns(3)
    r1.metric("VaR 90% (USD, Δ-normal)", f"{risk.get('VaR90', 0):,.2f}" if isinstance(risk.get("VaR90"), (int, float)) else "n/a")
    lam = risk.get("lambda")
    r2.metric("EWMA λ", f"{lam:.2f}" if isinstance(lam, (int, float)) else "—")
    r3.metric("Covariance", risk.get("cov_type") or "—")

# ---------- recent orders (last 2 minutes only) ----------
st.subheader("Recent Orders")
orders = (state.get("orders", []) or [])
orders = [o for o in orders if int(o.get("ts_ms", 0)) >= cutoff_ms]  # <<< filter to 2 minutes
if orders:
    # show most recent first, up to 100
    recent = list(reversed(orders))[:100]
    rows = []
    for o in recent:
        costs = _parse_costs(o.get("note", ""))
        rows.append({
            "Time (UTC)": fmt_ts(int(o.get("ts_ms", 0))),
            "Symbol": o.get("symbol"),
            "Side": o.get("side"),
            "Size": float(o.get("size", 0.0)),
            "Price": float(o.get("price", 0.0)),
            "Fee": costs.get("fee"),
            "½Spr": costs.get("½spr") or costs.get("half_spread"),
            "Slip": costs.get("slip"),
            "Cost": costs.get("cost"),
            "Venue": o.get("venue"),
            "Status": o.get("status"),
        })
    df_orders = pd.DataFrame(rows, columns=[
        "Time (UTC)", "Symbol", "Side", "Size", "Price",
        "Fee", "½Spr", "Slip", "Cost", "Venue", "Status"
    ])
    st.dataframe(df_orders, width="stretch", height=280,
                 column_config={
                     "Size": st.column_config.NumberColumn(format="%.6f"),
                     "Price": st.column_config.NumberColumn(format="%,.2f"),
                     "Fee": st.column_config.NumberColumn(format="%.2f"),
                     "½Spr": st.column_config.NumberColumn(format="%.2f"),
                     "Slip": st.column_config.NumberColumn(format="%.2f"),
                     "Cost": st.column_config.NumberColumn(format="%.2f"),
                 })
else:
    st.caption("No orders in the last 2 minutes.")

# =========================================================
# Session live history buffer (strict 2-minute trim)
# =========================================================
if "history" not in st.session_state:
    st.session_state.history = {
        "t": [],
        "realized": [],
        "unrealized": [],
        "total": [],
        "var90": [],
        "costs": [],
    }

H = st.session_state.history

# Append only if we have a new timestamp (prevents duplicates)
if ts_ms and (not H["t"] or H["t"][-1] != ts_ms):
    H["t"].append(ts_ms)
    H["realized"].append(float(state.get("realized_pnl", 0.0)))
    H["unrealized"].append(float(state.get("unrealized_pnl", 0.0)))
    H["total"].append(float(state.get("realized_pnl", 0.0) + state.get("unrealized_pnl", 0.0)))
    H["var90"].append(float(var90) if isinstance(var90, (int, float)) else None)
    H["costs"].append(float(state.get("transaction_costs", 0.0)))

# Strictly keep only last 2 minutes in session history
# (robust trim that handles empty/all-old cases)
if H["t"]:
    # find first index whose timestamp >= cutoff_ms
    keep_from = 0
    for i, t_ in enumerate(H["t"]):
        if int(t_) >= cutoff_ms:
            keep_from = i
            break
    else:
        # if loop didn't break, all points are older; drop everything
        keep_from = len(H["t"])
    for k in list(H.keys()):
        H[k] = H[k][keep_from:]

# ---------- P&L (live, last 2 minutes) ----------
st.subheader("P&L (live)")
metrics = state.get("metrics", []) or []
mrows = [m for m in metrics if int(m.get("t", 0)) >= cutoff_ms]  # <<< 2-minute filter

if mrows:
    dfpnl = pd.DataFrame(mrows)
    dfpnl["ts"] = pd.to_datetime(dfpnl["t"], unit="ms", utc=True)
    dfpnl = dfpnl.sort_values("ts")

    use_delta = st.toggle("Plot ΔP&L (from first point)", value=True, key="pnl_delta")
    dfpnl["total"] = dfpnl["realized"] + dfpnl["unrealized"]

    if use_delta:
        base = dfpnl.iloc[0]
        for c in ["realized", "unrealized", "total", "costs"]:
            if c in dfpnl.columns:
                dfpnl[c] = dfpnl[c] - float(base[c]) if pd.notna(base[c]) else dfpnl[c]
        y_suffix = "(Δ USD)"
    else:
        y_suffix = "(USD)"

    def one(series, title):
        if series not in dfpnl.columns:
            return None
        ser = dfpnl[["ts", series]].rename(columns={series: "val"})
        ql, qh = ser["val"].quantile([0.02, 0.98])
        pad = (qh - ql) * 0.15 or 1.0
        y_min, y_max = float(ql - pad), float(qh + pad)
        return (
            alt.Chart(ser)
            .mark_line()
            .encode(
                x=alt.X("ts:T", title=""),
                y=alt.Y("val:Q", title=f"{title} {y_suffix}",
                        scale=alt.Scale(domain=[y_min, y_max])),
                tooltip=[alt.Tooltip("ts:T", title="Time"),
                         alt.Tooltip("val:Q", title=title, format=",.2f")]
            )
            .properties(height=120, width="container")
        )

    charts = [one("realized", "Realized"),
              one("unrealized", "Unrealized"),
              one("total", "Total"),
              one("costs", "Costs")]
    charts = [c for c in charts if c is not None]
    if charts:
        st.altair_chart(alt.vconcat(*charts), width="stretch")
else:
    st.caption("No P&L points in the last 2 minutes.")

# ---------- VaR 90% (live, last 2 minutes) ----------
st.subheader("VaR 90% (live)")
vrows = [m for m in mrows if m.get("var90") is not None]
if vrows:
    dfvar = pd.DataFrame(vrows)
    dfvar["ts"] = pd.to_datetime(dfvar["t"], unit="ms", utc=True)
    dfvar = dfvar.sort_values("ts")
    ch = (
        alt.Chart(dfvar[["ts", "var90"]])
        .mark_line()
        .encode(
            x=alt.X("ts:T", title=""),
            y=alt.Y("var90:Q", title="USD"),
            tooltip=[alt.Tooltip("ts:T", title="Time"),
                     alt.Tooltip("var90:Q", title="VaR 90%", format=",.2f")]
        )
        .properties(height=180, width="container")
    )
    st.altair_chart(ch, width="stretch")
else:
    st.caption("No VaR history in the last 2 minutes.")

# ---------- health ----------
st.subheader("Health")
h1, h2 = st.columns(2)
h1.metric("Latency (ms)", lat_ms)
h2.caption(f"Last update: {ts_str}")

# ---------- controls ----------
if "auto_refresh" not in st.session_state:
    st.session_state.auto_refresh = True

c1, c2 = st.columns(2)
with c1:
    if st.button("Refresh"):
        st.rerun()
with c2:
    st.session_state.auto_refresh = st.toggle("Auto-refresh every 2s", value=st.session_state.auto_refresh)

if st.session_state.auto_refresh:
    time.sleep(2)
    st.rerun()
