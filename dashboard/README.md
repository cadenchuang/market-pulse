# dashboard/ (Streamlit)

`streamlit_app.py` visualizes **sentiment over time**, **flagged sentiment
shocks** (a heuristic — not a prediction), and **live pipeline metrics**.

```
dashboard/
├── streamlit_app.py   # the UI (the only file that imports streamlit)
├── data.py            # data access + transforms (no streamlit → unit-tested)
├── requirements.txt
├── Dockerfile
└── tests/             # tests for data.py against a temp SQLite DB
```

## What it shows

- **KPIs**: items scored, scored in the last 5 min, shocks flagged, average
  sentiment, and the active model (heuristic vs INT8 ONNX).
- **Sentiment over time**: average sentiment score + item volume, resampled into
  configurable time buckets.
- **Sentiment mix / by source**: label and source breakdowns.
- **Flagged shocks**: the `shocks` table (entity, z-score, velocity, time) —
  clearly labeled as a rolling-z-score heuristic.
- **Live pipeline metrics**: best-effort instant queries against Prometheus
  (throughput + consumer lag per stage); degrades gracefully to `—` when the
  metrics stack isn't running.

## How it reads data

`data.py` reads the **same SQLAlchemy tables the inference workers write**
(`sentiment_results`, `shocks`). In Docker they share a named volume
(`db-data`), and the dashboard mounts it read-only. There is no direct call
between services — the DB (and Prometheus) are the only coupling.

## Run

### With Docker (part of the full stack)

```bash
docker compose up --build
# open http://localhost:8501
```

### Locally

```bash
pip install -r dashboard/requirements.txt
DATABASE_URL=sqlite:///market_pulse.db PROMETHEUS_URL=http://localhost:9090 \
  PYTHONPATH=. streamlit run dashboard/streamlit_app.py
```

### Tests

```bash
pip install pandas sqlalchemy pytest
python -m pytest dashboard/tests
```

Config (env): `DATABASE_URL` (defaults to `sqlite:///market_pulse.db`) and
`PROMETHEUS_URL` (defaults to `http://localhost:9090`).
