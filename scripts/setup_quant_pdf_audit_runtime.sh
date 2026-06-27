#!/bin/bash
set -euo pipefail

RUNTIME_ROOT="${HOME}/.local/share/quant-pdf-audit"
VENV_PATH="${RUNTIME_ROOT}/venv"
PYTHON_BIN="${VENV_PATH}/bin/python"
PIP_BIN="${VENV_PATH}/bin/pip"
REQ_FILE="$(cd "$(dirname "$0")" && pwd)/requirements-quant-pdf-audit.txt"

mkdir -p "${RUNTIME_ROOT}"

if [ ! -d "${VENV_PATH}" ]; then
  python3 -m venv "${VENV_PATH}"
fi

"${PYTHON_BIN}" -m pip install --upgrade pip setuptools wheel
"${PIP_BIN}" install -r "${REQ_FILE}"

echo "quant-pdf-audit runtime ready"
echo "python=${PYTHON_BIN}"
"${PYTHON_BIN}" - <<'PY'
import importlib
modules = ["requests", "pypdf", "PyPDF2", "pdfplumber", "fitz", "pytesseract", "PIL"]
for name in modules:
    mod = importlib.import_module(name)
    print(f"{name}={getattr(mod, '__version__', 'unknown')}")
PY

if ! command -v tesseract >/dev/null 2>&1; then
  echo "warning: tesseract binary is missing; scanned PDF OCR audit will remain blocked"
  echo "macOS install hint: brew install tesseract tesseract-lang"
fi
