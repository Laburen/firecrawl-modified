#!/bin/sh

echo "🔍 Watchdog activo. Monitoreando containers unhealthy..."

while true; do
  for c in $(docker ps --filter "health=unhealthy" --format "{{.ID}}"); do
    echo "🔁 Reiniciando container unhealthy: $c"
    docker restart "$c"
  done
  sleep 3
done
