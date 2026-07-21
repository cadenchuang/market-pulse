"""Market Pulse Python inference layer.

Owns model inference only — sentiment + entity extraction over `news.processed`.
Kafka is the language boundary: this package never calls the Go services
directly, it consumes their topic output. See README.md for the processes-vs-
threads and partitioning rationale.
"""

__all__ = ["__version__"]

__version__ = "0.1.0"
