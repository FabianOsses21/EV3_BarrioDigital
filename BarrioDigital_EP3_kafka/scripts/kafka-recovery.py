"""Prueba de DLT, continuidad y deduplicación. Ejecutar con el Compose demo activo."""
import json
import subprocess
import time
import uuid
from datetime import datetime, timezone
from urllib.request import Request, urlopen

topic = "tramites-eventos"
broker = "kafka-1:9092"
key = str(uuid.uuid4())
event = {"eventId": str(uuid.uuid4()), "tramiteId": key, "eventType": "TRAMITE_INGRESADO",
         "occurredAt": datetime.now(timezone.utc).isoformat(), "estado": "INGRESADO",
         "estadoAnterior": None, "actor": "prueba-recuperacion", "correlationId": key}
marker = dict(event, eventId=str(uuid.uuid4()), eventType="ETAPA_CAMBIADA",
              estadoAnterior="INGRESADO", estado="ADMITIDO")
records = ["invalido-" + key, json.dumps(event), json.dumps(event), json.dumps(marker)]
command = ["docker", "compose", "exec", "-T", "kafka-1"]
subprocess.run(command + ["kafka-console-producer", "--bootstrap-server", broker, "--topic", topic,
                          "--property", "parse.key=true", "--property", "key.separator=|"],
               input="".join(key + "|" + value + "\n" for value in records), text=True, check=True, timeout=90)
request = Request("http://localhost:8080/api/notifications/kafka/" + key,
                  headers={"X-Demo-Role": "Admin", "X-Demo-User": "prueba-recuperacion"})
for attempt in range(60):
    with urlopen(request, timeout=5) as response:
        rows = json.load(response)
    if any(row["estado"] == "ADMITIDO" for row in rows):
        assert len(rows) == 2
        break
    time.sleep(1)
else:
    raise RuntimeError("El consumidor no alcanzó el evento posterior al duplicado")
result = subprocess.run(command + ["kafka-console-consumer", "--bootstrap-server", broker, "--topic",
                                   topic + "-dlt", "--from-beginning", "--timeout-ms", "5000"],
                        capture_output=True, text=True, timeout=45)
assert ("invalido-" + key) in result.stdout.splitlines(), "Falta el mensaje inválido en la DLT"
print("OK: DLT, continuidad e idempotencia", key)
