import sys
import os

# Allow imports like `from features.client_features import ...` when pytest
# is invoked from the repo root or the pipeline/ directory.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
