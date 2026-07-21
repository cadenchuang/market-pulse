# Streamlit dashboard. Build context is the repo root (see docker-compose.yml)
# so we can COPY the shared inference/storage models the dashboard reads.
FROM python:3.11-slim AS runtime
WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

COPY dashboard/requirements.txt dashboard/requirements.txt
RUN pip install --no-cache-dir -r dashboard/requirements.txt

# The dashboard imports inference.storage.* (shared ORM) + dashboard.*.
COPY inference/ inference/
COPY dashboard/ dashboard/

EXPOSE 8501
HEALTHCHECK --interval=15s --timeout=5s --retries=10 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health')" || exit 1

ENTRYPOINT ["streamlit", "run", "dashboard/streamlit_app.py", \
    "--server.address=0.0.0.0", "--server.port=8501", "--server.headless=true"]
