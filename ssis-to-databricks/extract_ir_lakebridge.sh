#!/usr/bin/env bash
# Extract Lakebridge's analyzer IR (JSON) from a folder of .dtsx files,
# bypassing the buggy Python `--generate-json` schema validation by calling
# the analyzer binary directly.
#
# Usage: ./extract_ir_lakebridge.sh <src_dtsx_dir> <out_dir>
set -euo pipefail

SRC="${1:?source dir of .dtsx files required}"
OUT="${2:?output dir required}"

# Locate the platform analyzer binary shipped inside the lakebridge venv.
BASE="$HOME/.databricks/labs/lakebridge/state/venv/lib/python3.13/site-packages/databricks/labs/bladespector/Analyzer"
case "$(uname -s)" in
  Darwin) BIN="$BASE/MacOS/analyzer" ;;
  Linux)  BIN="$BASE/Linux/analyzer" ;;
  *)      echo "Unsupported platform: $(uname -s)" >&2; exit 1 ;;
esac
[ -x "$BIN" ] || { echo "analyzer binary not found/executable at $BIN" >&2; exit 1; }

mkdir -p "$OUT"
# -d source dir, -r xlsx report, -t technology, -j json IR
UTF8_NOT_SUPPORTED=1 "$BIN" \
  -d "$SRC" \
  -r "$OUT/report.xlsx" \
  -t SSIS \
  -j "$OUT/ir.json"

echo "IR written to: $OUT/ir.json"
