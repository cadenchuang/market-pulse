# Python inference service. Targets Python 3.11 (ML wheels lag newer releases).
# Build context is the repo root (see docker-compose.yml).
FROM python:3.11-slim AS runtime
WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

COPY inference/requirements.txt inference/requirements.txt
RUN pip install --no-cache-dir -r inference/requirements.txt

# Optionally export + quantize FinBERT at build time (needs network + torch).
# Default false keeps builds fast and offline; runtime then uses the heuristic
# fallback until a model is provided via the mounted models volume.
ARG EXPORT_MODEL=false
COPY inference/ inference/
RUN if [ "$EXPORT_MODEL" = "true" ]; then \
      pip install --no-cache-dir -r inference/requirements-export.txt && \
      python -m inference.quantize --out inference/models/finbert-onnx ; \
    fi

# N worker processes, each with its own ONNX session, joined as a consumer group.
ENTRYPOINT ["python", "-m", "inference.consumer"]
