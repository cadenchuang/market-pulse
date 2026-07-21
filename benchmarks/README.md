# benchmarks/

Real, measured inference benchmarks — **never hardcoded**. `bench_inference.py`
measures per-item latency (p50/p95/mean) and batched throughput on CPU for
FinBERT **FP32 vs INT8**, writes a timestamped JSON + `latest.md` to `results/`,
and prints a markdown table.

## Run the real FP32 vs INT8 comparison

Requires the exported ONNX model and the ML stack (best on **Python 3.11**):

```bash
pip install -r inference/requirements.txt -r inference/requirements-export.txt
python -m inference.quantize --out inference/models/finbert-onnx   # one-time
python -m benchmarks.bench_inference --variants fp32,int8 --iterations 300
```

The table reports the measured **INT8 vs FP32 speedup** for this machine.

## Auto / offline mode

```bash
python -m benchmarks.bench_inference            # --variants auto
```

`auto` uses FP32+INT8 when a model is present, otherwise falls back to the
dependency-free heuristic model so the tooling stays runnable offline. The
fallback output is clearly labeled and is **not** a substitute for the real
FP32-vs-INT8 numbers.

## Method

- **Latency**: batch size 1, `--iterations` calls; p50/p95/mean over per-item
  times (after `--warmup` warmup calls).
- **Throughput**: batched calls of `--batch-size`; items/second.
- Inputs come from the synthetic replay dataset (`data/sample/news_sample.jsonl`).
- Environment (Python, OS, CPU, core count, timestamp) is recorded in the JSON so
  results are reproducible and comparable.

## Output

- `results/bench_<timestamp>.json` — full results + environment metadata.
- `results/latest.md` — the latest markdown table.

Result files are gitignored (they are generated per machine); commit the numbers
into the root README once you've run them on your target hardware.
