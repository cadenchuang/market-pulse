"""Export ProsusAI/finbert to ONNX and apply dynamic INT8 quantization.

Run once (or at image-build time); the result is cached on disk and loaded by
``model.OnnxFinbertModel`` at runtime. Heavy deps are imported lazily so the rest
of the package doesn't require them.

    python -m inference.quantize --out inference/models/finbert-onnx

The quantization is real (ONNX Runtime dynamic quantization). Latency/throughput
are measured for FP32 vs INT8 in Phase 6 (benchmarks/); nothing is hardcoded.
"""
from __future__ import annotations

import argparse
import logging
import os

logger = logging.getLogger(__name__)


def export_and_quantize(
    model_id: str = "ProsusAI/finbert",
    out_dir: str = "inference/models/finbert-onnx",
    force: bool = False,
) -> str:
    """Export ``model_id`` to ONNX + dynamic INT8, cached in ``out_dir``.

    Returns the output directory. Skips work if the quantized model already exists
    and ``force`` is False.
    """
    quantized_path = os.path.join(out_dir, "model_quantized.onnx")
    if os.path.exists(quantized_path) and not force:
        logger.info("quantized model already present at %s (use --force to rebuild)", quantized_path)
        return out_dir

    # Lazy imports: only needed when actually exporting.
    from optimum.onnxruntime import ORTModelForSequenceClassification, ORTQuantizer
    from optimum.onnxruntime.configuration import AutoQuantizationConfig
    from transformers import AutoTokenizer

    os.makedirs(out_dir, exist_ok=True)

    logger.info("exporting %s to ONNX (FP32)...", model_id)
    model = ORTModelForSequenceClassification.from_pretrained(model_id, export=True)
    model.save_pretrained(out_dir)
    AutoTokenizer.from_pretrained(model_id).save_pretrained(out_dir)

    logger.info("applying dynamic INT8 quantization...")
    quantizer = ORTQuantizer.from_pretrained(out_dir)
    qconfig = AutoQuantizationConfig.avx512_vnni(is_static=False, per_channel=False)
    quantizer.quantize(save_dir=out_dir, quantization_config=qconfig)

    logger.info("done: %s", out_dir)
    return out_dir


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--model-id", default=os.environ.get("MODEL_ID", "ProsusAI/finbert"))
    ap.add_argument("--out", default=os.environ.get("ONNX_MODEL_DIR", "inference/models/finbert-onnx"))
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()
    export_and_quantize(args.model_id, args.out, args.force)


if __name__ == "__main__":
    main()
