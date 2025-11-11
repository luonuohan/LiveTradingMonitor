# services/risk.py
import asyncio, time, math, numpy as np
from collections import defaultdict

from core.bus import BUS
from core.schemas import Tick
from core.state import STORE, SharedState, append_metric

# --- Risk knobs ---
LAMBDA = 0.94            # EWMA decay (RiskMetrics)
Z90    = 1.2815515655    # 90% z
HORIZON_TICKS = 60       # VaR horizon in number of ticks (scale variance)
MIN_OBS = 5              # emit as soon as we have a few returns
APPEND_METRICS_EVERY_MS = 2000  # throttle metrics persistence

class EwmaCov:
    def __init__(self, lam=LAMBDA):
        self.lam = lam
        self.mu   = {}                 # running mean per symbol
        self.sigs = {}                 # running variance per symbol
        self.cov  = defaultdict(dict)  # cov matrix dict-of-dicts
        self.last_px = {}

    def update(self, sym, px):
        """Return log-return and update EWMA moments for a single symbol."""
        px0 = self.last_px.get(sym)
        self.last_px[sym] = float(px)
        if px0 is None or px0 <= 0.0:
            return None  # need a previous price

        r = math.log(px / px0)

        lam = self.lam
        mu_old = self.mu.get(sym, 0.0)
        mu_new = lam * mu_old + (1 - lam) * r
        self.mu[sym] = mu_new

        sig_old = self.sigs.get(sym, 0.0)
        # variance of demeaned process (RiskMetrics-style)
        var_new = lam * sig_old + (1 - lam) * (r - mu_new) ** 2
        self.sigs[sym] = var_new

        return r

    def update_pair(self, s1, r1, s2, r2):
        """EWMA covariance update for a pair using current demeaned returns."""
        lam = self.lam
        if r1 is None or r2 is None:
            return
        m1 = self.mu.get(s1, 0.0); m2 = self.mu.get(s2, 0.0)
        c_old = self.cov[s1].get(s2, 0.0)
        c_new = lam * c_old + (1 - lam) * ((r1 - m1) * (r2 - m2))
        self.cov[s1][s2] = c_new
        self.cov[s2][s1] = c_new  # symmetry

    def symbols(self):
        return list(self.last_px.keys())

RISK = EwmaCov(LAMBDA)

async def run_risk():
    q = asyncio.Queue()
    BUS.subscribe("tick", q)

    last_ret = {}                # latest return per symbol (for pair updates)
    last_metrics_ms = 0          # throttle for append_metric

    while True:
        t: Tick = await q.get()
        s = t.symbol
        px = float(t.price)

        # 1) update EWMA stats for symbol; get its return
        r = RISK.update(s, px)
        if r is None:
            continue
        last_ret[s] = r

        # 2) update pairwise covariances vs other symbols with a recent return
        for s2, r2 in list(last_ret.items()):
            if s2 != s:
                RISK.update_pair(s, r, s2, r2)

        # 3) compute VaR when we have enough observations
        st: SharedState = STORE.read()
        syms = sorted(set(list(st.last_prices.keys())))
        if not syms:
            continue

        # require non-zero variance for all symbols we care about
        if any(RISK.sigs.get(sy, 0.0) == 0.0 for sy in syms):
            continue

        # USD weights = qty * last_price
        w = np.array(
            [st.positions.get(sy, 0.0) * st.last_prices.get(sy, 0.0) for sy in syms],
            dtype=float
        )

        if not np.any(w):   # flat book => VaR = 0
            var90 = 0.0
        else:
            # build EWMA covariance matrix
            N = len(syms)
            C = np.zeros((N, N), dtype=float)
            for i, si in enumerate(syms):
                C[i, i] = RISK.sigs.get(si, 0.0)
                for j in range(i + 1, N):
                    sj = syms[j]
                    cij = RISK.cov[si].get(sj, 0.0)
                    C[i, j] = C[j, i] = cij
            # portfolio variance per tick in USD (delta-normal approximation)
            var_tick = float(w @ C @ w.T)
            # project to horizon (ticks) and apply z for 90%
            var_h = max(var_tick, 0.0) * max(HORIZON_TICKS, 1)
            var90 = Z90 * math.sqrt(var_h)

        # 4) write risk snapshot (small dict; UI reads VaR90 from here)
        st = STORE.read()
        st.__dict__["risk"] = {
            "symbols": syms,
            "VaR90": var90,
            "cov_type": "EWMA",
            "lambda": LAMBDA,
        }
        STORE.write(st)

        # 5) persist a metrics row every ~2s (kept to last 10 min by append_metric/state.py)
        now_ms = int(time.time() * 1000)
        if now_ms - last_metrics_ms >= APPEND_METRICS_EVERY_MS:
            st_now = STORE.read()
            append_metric(
                ts_ms=now_ms,
                realized=st_now.realized_pnl,
                unrealized=st_now.unrealized_pnl,
                costs=getattr(st_now, "transaction_costs", 0.0) or 0.0,
                var90=st_now.risk.get("VaR90") if isinstance(st_now.risk, dict) else None,
            )
            last_metrics_ms = now_ms
