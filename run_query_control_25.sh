#!/usr/bin/env bash
set -euo pipefail

# Paths relative to repo root; override with env vars. First arg puede indicar query (AGING_1..4 o path).
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${CONFIG_FILE:-$ROOT_DIR/query_control/configs/query1.config}"
DEFAULT_QUERY="$ROOT_DIR/queries/AGING_1.query"
ARG_QUERY="${1:-}"

if [[ -n "$ARG_QUERY" ]]; then
  case "$ARG_QUERY" in
    1|AGING_1) QUERY_FILE="$ROOT_DIR/queries/AGING_1.query" ;;
    2|AGING_2) QUERY_FILE="$ROOT_DIR/queries/AGING_2.query" ;;
    3|AGING_3) QUERY_FILE="$ROOT_DIR/queries/AGING_3.query" ;;
    4|AGING_4) QUERY_FILE="$ROOT_DIR/queries/AGING_4.query" ;;
    *.query|*/*) QUERY_FILE="$ARG_QUERY" ;; # usa path directo
    *)
      echo "Query inválida: $ARG_QUERY. Usá AGING_1..4, 1..4 o un path .query" >&2
      exit 1
      ;;
  esac
else
  QUERY_FILE="${QUERY_FILE:-$DEFAULT_QUERY}"
fi
PRIORITY="${PRIORITY:-1}"
COUNT="${COUNT:-25}"
SLEEP_SECS="${SLEEP_SECS:-0}"

BIN="$ROOT_DIR/query_control/bin/query_control"

if [[ ! -x "$BIN" ]]; then
  echo "No se encontró el binario $BIN. Compilá con: make -C query_control" >&2
  exit 1
fi

echo "Lanzando $COUNT ejecuciones en paralelo..."
pids=()
for i in $(seq 1 "$COUNT"); do
  echo "[$i/$COUNT] Ejecutando query_control..."
  "$BIN" "$CONFIG_FILE" "$QUERY_FILE" "$PRIORITY" &
  pids+=("$!")
  (( SLEEP_SECS > 0 )) && sleep "$SLEEP_SECS" # delay opcional entre lanzamientos
done

echo "Esperando a que terminen las ejecuciones..."
for pid in "${pids[@]}"; do
  wait "$pid"
done

echo "Listo: $COUNT ejecuciones completadas."
