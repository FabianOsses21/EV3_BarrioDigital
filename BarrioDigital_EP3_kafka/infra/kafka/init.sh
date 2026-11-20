#!/bin/bash
set -eu
servers=kafka-1:9092,kafka-2:9092,kafka-3:9092
for topic in tramites-eventos tramites-auditoria tramites-eventos-dlt tramites-auditoria-dlt; do
  retention=604800000
  if [ "$topic" = tramites-auditoria ]; then retention=2592000000; fi
  ready=false
  for attempt in {1..30}; do
    if kafka-topics --bootstrap-server "$servers" --create --if-not-exists --topic "$topic" --partitions 3 --replication-factor 3 --config min.insync.replicas=2 --config cleanup.policy=delete --config retention.ms="$retention"; then ready=true; break; fi
    sleep 3
  done
  "$ready" || exit 1
  # --create --if-not-exists no reconcilia un tópico ya existente: se
  # reafirma la configuración esperada para corregir desvíos previos.
  kafka-configs --bootstrap-server "$servers" --entity-type topics --entity-name "$topic" --alter \
    --add-config "min.insync.replicas=2,cleanup.policy=delete,retention.ms=$retention"
done
