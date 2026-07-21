import os
import sys

# Ensure the repo root is importable so `import dashboard` / `import inference` work.
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
