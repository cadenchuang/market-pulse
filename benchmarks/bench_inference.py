"""Benchmark FinBERT inference: FP32 vs INT8, real measured latency + throughput.

Measures per-item latency (p50/p95/mean) and batched throughput on CPU, writes a
timestamped JSON + a markdown table to benchmarks/results/, and prints the table.

Nothing is hardcoded — every number comes from a real run on the current machine.
The INT8 model must exist (run ``python -m inference.quantize`` first). If no ONNX
model is available, the harness falls back to benchmarking the dependency-free
heuristic model so the tooling itself stays runnable offline; that fallback is
clearly labeled and is NOT a substitute for the FP32-vs-INT8 comparison.

Usage:
    python -m benchmarks.bench_inference --variants auto --iterations 200
    python -m benchmarks.bench_inference --variants fp32,int8 --model-dir inference/models/finbert-onnx
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter

# Allow running as a script (python benchmarks/bench_inference.py) as well as a
# module (python -m benchmarks.bench_inference).
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from inference.model import HeuristicModel, SentimentModel  # noqa: E402

RESULTS_DIR = REPO_ROOT / "benchmarks" / "results"


def percentile(values: list[float], p: float) -> float:
    """Linear-interpolated percentile (p in [0, 100]). Pure stdlib."""
    if not values:
        raise ValueError("percentile of empty sequence")
    xs = sorted(values)
    if len(xs) == 1:
        return xs[0]
    k = (len(xs) - 1) * (p / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(xs) - 1)
    frac = k - lo
    return xs[lo] * (1 - frac) + xs[hi] * frac


@dataclass
class BenchResult:
    variant: str
    model_name: str
    quantized: bool
    iterations: int
    batch_size: int
    p50_ms: float
    p95_ms: float
    mean_ms: float
    throughput_items_per_s: float


def measure_latency_ms(model: SentimentModel, texts: list[str], iterations: int, warmup: int) -> list[float]:
    """Per-item latency (batch size 1), in milliseconds."""
    for i in range(warmup):
        model.predict([texts[i % len(texts)]])
    out: list[float] = []
    for i in range(iterations):
        t0 = perf_counter()
        model.predict([texts[i % len(texts)]])
        out.append((perf_counter() - t0) * 1000.0)
    return out


def measure_throughput(model: SentimentModel, texts: list[str], batch_size: int, iterations: int, warmup: int) -> float:
    """Items per second over batched calls."""
    batch = [texts[i % len(texts)] for i in range(batch_size)]
    for _ in range(warmup):
        model.predict(batch)
    items = 0
    t0 = perf_counter()
    for _ in range(iterations):
        model.predict(batch)
        items += batch_size
    dur = perf_counter() - t0
    return items / dur if dur > 0 else 0.0


def run_benchmark(
    variant: str,
    model: SentimentModel,
    texts: list[str],
    *,
    iterations: int,
    batch_size: int,
    warmup: int,
) -> BenchResult:
    latencies = measure_latency_ms(model, texts, iterations, warmup)
    throughput = measure_throughput(model, texts, batch_size, max(1, iterations // 4), warmup)
    return BenchResult(
        variant=variant,
        model_name=getattr(model, "name", "unknown"),
        quantized=getattr(model, "quantized", False),
        iterations=iterations,
        batch_size=batch_size,
        p50_ms=percentile(latencies, 50),
        p95_ms=percentile(latencies, 95),
        mean_ms=sum(latencies) / len(latencies),
        throughput_items_per_s=throughput,
    )


def build_variant(name: str, model_dir: str) -> SentimentModel | None:
    """Instantiate a model variant, or return None (with a message) if unavailable."""
    if name == "heuristic":
        return HeuristicModel()
    if name in ("fp32", "int8"):
        try:
            from inference.model import OnnxFinbertModel

            return OnnxFinbertModel(model_dir, quantized=(name == "int8"))
        except Exception as exc:
            print(f"  [skip] variant '{name}' unavailable: {exc}", file=sys.stderr)
            return None
    raise ValueError(f"unknown variant: {name}")


def resolve_variants(requested: str, model_dir: str) -> list[str]:
    if requested != "auto":
        return [v.strip() for v in requested.split(",") if v.strip()]
    # auto: prefer the real FP32-vs-INT8 comparison if a model is present.
    if os.path.isdir(model_dir) and os.path.exists(os.path.join(model_dir, "model.onnx")):
        return ["fp32", "int8"]
    print("  [info] no ONNX model found; falling back to heuristic model", file=sys.stderr)
    return ["heuristic"]


def load_texts(data_path: Path, limit: int) -> list[str]:
    texts: list[str] = []
    if data_path.exists():
        for line in data_path.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            title = obj.get("title", "")
            body = obj.get("body", "")
            texts.append(f"{title}. {body}".strip())
            if len(texts) >= limit:
                break
    if not texts:
        texts = [
            "Company beats estimates as quarterly revenue jumps to a record high.",
            "Company misses estimates and cuts its outlook on weak demand.",
            "Company names a new chief financial officer effective next month.",
        ]
    return texts


def environment() -> dict:
    return {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "processor": platform.processor() or platform.machine(),
        "cpu_count": os.cpu_count(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def format_markdown(results: list[BenchResult], env: dict) -> str:
    lines = [
        "# Inference benchmark results",
        "",
        f"- Generated: `{env['timestamp']}`",
        f"- Python: `{env['python']}` on `{env['platform']}`",
        f"- CPU: `{env['processor']}` ({env['cpu_count']} logical cores)",
        "",
        "| Variant | Model | p50 latency (ms) | p95 latency (ms) | mean (ms) | throughput (items/s) |",
        "|---------|-------|------------------|------------------|-----------|----------------------|",
    ]
    for r in results:
        lines.append(
            f"| {r.variant} | `{r.model_name}` | {r.p50_ms:.2f} | {r.p95_ms:.2f} | "
            f"{r.mean_ms:.2f} | {r.throughput_items_per_s:.1f} |"
        )

    by_variant = {r.variant: r for r in results}
    if "fp32" in by_variant and "int8" in by_variant:
        fp32, int8 = by_variant["fp32"], by_variant["int8"]
        speedup = fp32.p50_ms / int8.p50_ms if int8.p50_ms else 0.0
        tput = int8.throughput_items_per_s / fp32.throughput_items_per_s if fp32.throughput_items_per_s else 0.0
        lines += [
            "",
            f"**INT8 vs FP32:** {speedup:.2f}x lower p50 latency, "
            f"{tput:.2f}x throughput (measured, this machine).",
        ]
    else:
        lines += [
            "",
            "> Note: these are fallback/heuristic numbers. For the real FP32-vs-INT8 "
            "comparison, export the model (`python -m inference.quantize`) on Python "
            "3.11 and re-run with `--variants fp32,int8`.",
        ]
    return "\n".join(lines) + "\n"


def write_results(results: list[BenchResult], env: dict, out_dir: Path) -> tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    json_path = out_dir / f"bench_{stamp}.json"
    md_path = out_dir / "latest.md"

    payload = {"environment": env, "results": [asdict(r) for r in results]}
    json_path.write_text(json.dumps(payload, indent=2) + "\n")
    md_path.write_text(format_markdown(results, env))
    return json_path, md_path


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--variants", default="auto", help="'auto', or comma list of: fp32,int8,heuristic")
    ap.add_argument("--model-dir", default=os.environ.get("ONNX_MODEL_DIR", "inference/models/finbert-onnx"))
    ap.add_argument("--data", default=str(REPO_ROOT / "data" / "sample" / "news_sample.jsonl"))
    ap.add_argument("--iterations", type=int, default=200)
    ap.add_argument("--batch-size", type=int, default=16)
    ap.add_argument("--warmup", type=int, default=10)
    ap.add_argument("--limit-texts", type=int, default=64)
    ap.add_argument("--out-dir", default=str(RESULTS_DIR))
    args = ap.parse_args()

    texts = load_texts(Path(args.data), args.limit_texts)
    variants = resolve_variants(args.variants, args.model_dir)
    env = environment()

    results: list[BenchResult] = []
    for name in variants:
        model = build_variant(name, args.model_dir)
        if model is None:
            continue
        print(f"benchmarking '{name}' ({getattr(model, 'name', '?')})...", file=sys.stderr)
        results.append(
            run_benchmark(
                name, model, texts,
                iterations=args.iterations, batch_size=args.batch_size, warmup=args.warmup,
            )
        )

    if not results:
        print("no variants could be benchmarked", file=sys.stderr)
        sys.exit(1)

    json_path, md_path = write_results(results, env, Path(args.out_dir))
    print(format_markdown(results, env))
    print(f"wrote {json_path}\nwrote {md_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
