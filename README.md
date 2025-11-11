# LiveTradingMonitor — Real-Time Risk & P&L Dashboard (Segmented State Architecture)

A production-minded, real-time trading monitor that streams multi-symbol prices, computes Δ-normal VaR with EWMA covariance, tracks P&L/latency/costs, and **runs indefinitely** thanks to a **segmented (dual) JSON state buffer** that prevents file bloat.

![UI](assets/ui.png)

---

## ✨ Highlights
- **Live monitoring:** P&L (realized/unrealized), VaR(90%), latency, and execution costs across BTC, ETH, and more.
- **Risk engine:** EWMA (λ=0.94) covariance; Δ-normal VaR with configurable horizon.
- **Segmented state buffer:** rotating **2-minute JSON segments** + snapshot scalars; enables 24/7 uptime without memory growth.
- **Event-driven strategy:** Dual-SMA band with slope bias, micro-breakout confirmation, cooldown/min-hold, and trailing/stop-loss.

---

## 🧭 Architecture (high level)

```mermaid
flowchart LR
  FEED[Market Feed / Sim] -->|ticks| MON[Monitor]
  MON -->|update_last_price / append_intraday| STATE[(state.json)]
  MON --> SEG[segments/ (2-min buffers)]
  STRAT[Stateful Strategy] -->|signals| BUS[(Async BUS)]
  BUS --> EXEC[Execution + Costs]
  EXEC -->|acks/costs| STATE
  RISK[EWMA VaR Engine] -->|metrics| STATE
  RISK --> SEG
  STATE --> UI[Streamlit Dashboard]
  SEG --> UI
