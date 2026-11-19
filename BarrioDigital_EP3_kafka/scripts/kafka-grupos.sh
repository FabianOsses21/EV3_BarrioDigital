#!/bin/sh
set -eu
for group in barrio-seguimiento barrio-auditoria; do
  for detail in --state --members --offsets; do
    docker compose exec -T kafka-1 kafka-consumer-groups --bootstrap-server kafka-1:9092 --describe --group "$group" "$detail"
  done
done
