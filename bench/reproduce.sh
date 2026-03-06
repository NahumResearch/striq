#!/usr/bin/env bash
# reproduce.sh — Download datasets, build STRIQ, run benchmarks, generate plots.
#
# Usage:
#   cd <repo-root>
#   bash bench/reproduce.sh
#
# Requirements:
#   - C11 compiler (clang or gcc)
#   - make, curl, unzip
#   - lz4 and zstd libraries (for competitor benchmarks)
#     Ubuntu: sudo apt-get install -y liblz4-dev libzstd-dev
#     macOS:  brew install lz4 zstd
#   - Python 3 + plotly + kaleido (for plots)
#     pip install plotly kaleido

set -euo pipefail
REPO="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO"

echo "=== STRIQ Benchmark Reproduce ==="
echo ""

echo "--- 1. Datasets ---"
bash bench/datasets/download.sh
echo ""

echo "--- 2. Build ---"
make clean
make
echo ""

echo "--- 3. Tests ---"
make test
echo ""

echo "--- 4. Benchmarks ---"
make bench
echo ""
make bench_epsilon
echo ""

echo "--- 5. Plots ---"
if python3 -c "import plotly" 2>/dev/null; then
    python3 bench/plot.py
else
    echo "Skipping plots: pip install plotly kaleido"
fi

echo ""
echo "=== Done — results in bench/results/ ==="
