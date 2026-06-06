#!/usr/bin/env bash
set -euo pipefail

# Assumes uv is already installed.
# If not, install it first: https://docs.astral.sh/uv/

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

uv init --python 3.12 --package .
uv venv --python 3.12

source .venv/bin/activate
uv pip install --upgrade pip

echo "Project initialized in $SCRIPT_DIR"
echo "Virtual environment: $SCRIPT_DIR/.venv"
echo "Activate later with: source .venv/bin/activate"
