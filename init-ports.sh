#!/usr/bin/env bash
# Scans for occupied host ports (docker + host sockets) and writes
# docker-compose.override.yml remapping only the services whose
# declared port is already taken.
set -euo pipefail
cd "$(dirname "$0")"

OVERRIDE="docker-compose.override.yml"

# service:host:container  (promtail has no published port -> omitted)
SERVICES=(
  "loki:3100:3100"
  "grafana:3000:3000"
  "postgres:5433:5432"
  "pgadmin:5050:5050"
)

taken_ports() {
  # docker-published host ports
  docker ps --format '{{.Ports}}' 2>/dev/null \
    | grep -oE '0\.0\.0\.0:[0-9]+|:::[0-9]+|\[::\]:[0-9]+' \
    | grep -oE '[0-9]+' || true
  # host listening sockets
  if command -v ss >/dev/null 2>&1; then
    ss -tlnH 2>/dev/null | awk '{print $4}' | grep -oE '[0-9]+$' || true
  elif command -v lsof >/dev/null 2>&1; then
    lsof -iTCP -sTCP:LISTEN -P -n 2>/dev/null | grep -oE ':[0-9]+ ' | grep -oE '[0-9]+' || true
  fi
}

mapfile -t TAKEN < <(taken_ports | sort -un)

declare -a REMAP=()              # "service:newhost:container:oldhost"
declare -a USED=("${TAKEN[@]}")  # taken + newly assigned, grows as we go

is_used() {
  local p=$1 t
  for t in "${USED[@]}"; do [ "$t" = "$p" ] && return 0; done
  return 1
}

for entry in "${SERVICES[@]}"; do
  IFS=: read -r svc host cont <<<"$entry"
  port=$host
  changed=0
  while is_used "$port"; do
    port=$((port + 1))
    changed=1
  done
  USED+=("$port")
  if [ "$changed" = 1 ]; then
    REMAP+=("$svc:$port:$cont:$host")
  fi
done

if [ ${#REMAP[@]} -eq 0 ]; then
  echo "All declared ports free. No override needed."
  rm -f "$OVERRIDE"
  exit 0
fi

{
  echo "services:"
  for r in "${REMAP[@]}"; do
    IFS=: read -r svc new cont old <<<"$r"
    printf '  %s:\n    ports:\n      - "%s:%s"\n' "$svc" "$new" "$cont"
  done
} > "$OVERRIDE"

echo "Wrote $OVERRIDE:"
for r in "${REMAP[@]}"; do
  IFS=: read -r svc new cont old <<<"$r"
  echo "  $svc  $old -> $new"
done
