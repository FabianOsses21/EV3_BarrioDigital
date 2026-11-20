#!/bin/sh
set -eu
# Uso local (raíz del proyecto):   ./scripts/kafka-grupos.sh
# EC2 (compose.cloud.yml):         COMPOSE_PROJECT=barriodigital-ev3 \
#                                  COMPOSE_FILES="-f compose.yml -f compose.cloud.yml" \
#                                  ./scripts/kafka-grupos.sh
for group in barrio-seguimiento barrio-auditoria; do
  for detail in --state --members --offsets; do
    docker compose ${COMPOSE_PROJECT:+-p "$COMPOSE_PROJECT"} ${COMPOSE_FILES:-} \
      exec -T kafka-1 kafka-consumer-groups --bootstrap-server kafka-1:9092 \
      --describe --group "$group" "$detail"
  done
done
