#!/usr/bin/env bash
set -euo pipefail

# Paths relative to repo root; override with env vars if needed.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${CONFIG_FILE:-$ROOT_DIR/query_control/configs/query1.config}"
QUERY_FILE="${QUERY_FILE:-$ROOT_DIR/queries/STORAGE_1.query}"
PRIORITY="${PRIORITY:-1}"
COUNT="${COUNT:-25}"
SLEEP_SECS="${SLEEP_SECS:-0}"

BIN="$ROOT_DIR/query_control/bin/query_control"

if [[ ! -x "$BIN" ]]; then
  echo "No se encontró el binario $BIN. Compilá con: make -C query_control" >&2
  exit 1
fi

for i in $(seq 1 "$COUNT"); do
  echo "[$i/$COUNT] Ejecutando query_control..."
  "$BIN" "$CONFIG_FILE" "$QUERY_FILE" "$PRIORITY"
  (( SLEEP_SECS > 0 )) && sleep "$SLEEP_SECS"
done
