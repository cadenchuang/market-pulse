"""Market Pulse dashboard (Streamlit).

Visualizes sentiment over time, flagged sentiment shocks (a heuristic — NOT a
prediction), and live pipeline metrics. Reads the same results DB the inference
workers write, and queries Prometheus for live throughput / consumer lag.

All data access + transforms live in ``dashboard/data.py`` (no Streamlit import),
so they are unit-tested independently of the UI.
"""
from __future__ import annotations

import os

import pandas as pd
import streamlit as st

from dashboard import data

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///market_pulse.db")
PROMETHEUS_URL = os.environ.get("PROMETHEUS_URL", "http://localhost:9090")

st.set_page_config(page_title="Market Pulse", page_icon="📈", layout="wide")


@st.cache_resource
def _engine():
    return data.get_engine(DATABASE_URL)


def main() -> None:
    st.title("📈 Market Pulse")
    st.caption(
        "Real-time financial news sentiment. Sentiment **shocks** below are a "
        "heuristic (rolling z-score of per-entity sentiment velocity) — not a "
        "market prediction."
    )

    with st.sidebar:
        st.header("Controls")
        limit = st.slider("Rows to load", 100, 5000, 2000, step=100)
        bucket = st.selectbox("Timeline bucket", ["30s", "1min", "5min", "15min"], index=1)
        refresh = st.number_input("Auto-refresh (seconds, 0 = off)", 0, 300, 10)
        st.write("DB:", f"`{DATABASE_URL}`")

    engine = _engine()
    results = data.load_results(engine, limit=limit)
    shocks = data.load_shocks(engine, limit=200)
    # DB-wide totals so KPIs stay accurate beyond the loaded row window.
    stats = data.global_stats(engine)
    kpis = data.compute_kpis(results, shocks, stats=stats)

    # --- KPIs ---------------------------------------------------------------
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Items scored", f"{kpis.total_scored:,}")
    c2.metric("Scored (last 5m)", f"{kpis.scored_last_5m:,}")
    c3.metric("Shocks flagged", f"{kpis.shocks_flagged:,}")
    c4.metric("Avg sentiment", f"{kpis.avg_sentiment:+.3f}")
    model = kpis.model_name + (" (INT8)" if kpis.quantized else "")
    c5.metric("Model", model)

    # --- Live pipeline metrics (best-effort from Prometheus) ----------------
    with st.expander("Live pipeline metrics (Prometheus)", expanded=False):
        _render_prometheus()
        st.caption(f"Full dashboards in Grafana. Prometheus: {PROMETHEUS_URL}")

    if results.empty:
        st.info("No results yet. Start the pipeline (`docker compose up`) and wait for items to be scored.")
        _maybe_refresh(refresh)
        return

    # --- Sentiment over time ------------------------------------------------
    st.subheader("Sentiment over time")
    timeline = data.sentiment_timeline(results, rule=bucket)
    left, right = st.columns([2, 1])
    with left:
        st.line_chart(timeline[["avg_sentiment"]], height=280)
        st.bar_chart(timeline[["volume"]], height=180)
    with right:
        st.caption("Sentiment mix")
        st.bar_chart(data.label_distribution(results).set_index("label"), height=280)
        st.caption("By source")
        st.bar_chart(data.source_distribution(results).set_index("source"), height=180)

    # --- Flagged shocks -----------------------------------------------------
    st.subheader("Flagged sentiment shocks (heuristic)")
    if shocks.empty:
        st.info("No shocks flagged yet.")
    else:
        show = shocks.copy()
        show["zscore"] = show["zscore"].round(2)
        show["velocity"] = show["velocity"].round(3)
        show["sentiment_score"] = show["sentiment_score"].round(3)
        st.dataframe(
            show[["triggered_at", "entity", "zscore", "velocity", "sentiment_score", "item_id"]],
            use_container_width=True,
            hide_index=True,
        )

    # --- Recent items -------------------------------------------------------
    st.subheader("Recent scored items")
    recent = results.head(100)[
        ["inferred_at", "source", "sentiment_label", "sentiment_score", "item_id"]
    ].copy()
    recent["sentiment_score"] = recent["sentiment_score"].round(3)
    st.dataframe(recent, use_container_width=True, hide_index=True)

    _maybe_refresh(refresh)


def _render_prometheus() -> None:
    queries = {
        "Ingestor msg/s": "sum(rate(market_pulse_ingestor_produced_total[1m]))",
        "Processor msg/s": "sum(rate(market_pulse_processor_processed_total[1m]))",
        "Inference items/s": "sum(rate(market_pulse_inference_processed_total[1m]))",
        "Processor lag": "market_pulse_processor_consumer_lag",
        "Inference lag": "sum(market_pulse_inference_consumer_lag)",
    }
    cols = st.columns(len(queries))
    any_data = False
    for col, (label, q) in zip(cols, queries.items()):
        val = data.fetch_prometheus_scalar(PROMETHEUS_URL, q)
        if val is None:
            col.metric(label, "—")
        else:
            any_data = True
            col.metric(label, f"{val:,.2f}")
    if not any_data:
        st.caption("Prometheus not reachable (metrics stack may be off).")


def _maybe_refresh(seconds: int) -> None:
    if seconds and seconds > 0:
        # Lightweight auto-refresh without extra deps.
        st.markdown(
            f"<meta http-equiv='refresh' content='{int(seconds)}'>",
            unsafe_allow_html=True,
        )


if __name__ == "__main__":
    main()
