# BarrioDigital EP3 · Kafka y RabbitMQ

## Arranque local
Desde la raíz del proyecto con Docker Compose v2 reciente y Docker Linux:
```powershell
Copy-Item .env.example .env
docker compose -f compose.yml -f compose.demo.yml up -d --build
docker compose ps -a
python scripts/smoke.py --mode demo
python scripts/kafka-recovery.py
```
Copiar la plantilla solo en el primer arranque. La demo acepta roles por cabecera y debe ejecutarse con acceso local. Abrir Angular en http://localhost:4200 y Kafka UI en http://localhost:8090. El gateway escucha en 8080. El contenedor kafka-init debe terminar con código 0. Los volúmenes conservan el estado entre reinicios.

## EC2 con Entra ID
1. Copiar el proyecto completo, incluida .env.example, a una instancia Linux con Docker Engine y Compose v2. Reservar memoria para seis JVM de aplicación, seis nodos Kafka/ZooKeeper, RabbitMQ y el build Maven/Angular. Como punto inicial para el laboratorio, considerar 16 GiB y verificar con docker stats. Oracle XE añade consumo.
2. En la raíz, ejecutar `cp -n .env.example .env` y editar las claves RabbitMQ y la cookie de Erlang para una instalación nueva. Para una actualización reutilizar la configuración, el nombre de proyecto y los volúmenes existentes. Respaldar las bases antes de actualizar.
3. Configurar CORS_ORIGINS con el origen HTTPS real, registrar su URI SPA en Entra y comprobar roles y scope OT.Create. Con proxy HTTPS delante de Angular, PUBLIC_API_URL vacío usa /api del mismo origen. Si se usa AWS API Gateway, indicar su URL y configurar su integración según docs/ENTRA_Y_AWS.md.
4. Levantar con `docker compose -f compose.yml -f compose.cloud.yml up -d --build`. El override publica frontend 4200 y gateway 8080: limitar el ingreso a su proxy o integración. Kafka UI y RabbitMQ Management son locales. Para Kafka UI abrir un túnel desde el equipo: `ssh -i clave.pem -L 8090:localhost:8090 ubuntu@HOST_EC2`. Abrir http://localhost:8090. Mantener internos los brokers y ZooKeeper. El TLS público requiere configurar un proxy HTTPS o balanceador propio.
5. Comprobar `docker compose ps -a`, `docker compose logs kafka-init requests notify kafkaadmin` y el ingreso desde Angular. Para ejecutar el smoke con Entra definir ACCESS_TOKEN con un token de administrador y ejecutar `python3 scripts/smoke.py --mode entra`. La prueba integral usa la opción DEMO_FAILURE_ENABLED para probar el error de correo de EV2.
6. H2 es la opción base. Para Oracle nuevo, añadir `-f compose.oracle.yml` y configurar los secretos de Oracle en .env. Si hay un esquema notify previo, aplicar una vez infra/oracle/ep3_upgrade_notify.sql conectado como su dueño antes del reinicio. En Oracle existente establecer ORACLE_JDBC_URL con el servicio real. El script de creación completa es para volúmenes nuevos.

## Flujo de un trámite
Requests guarda el trámite, tres comandos AMQP y dos registros Kafka en una transacción. Cada despachador lee solo su transporte; el de Kafka espera la confirmación del broker antes de marcar el registro. La clave Kafka es el UUID del trámite. Un reintento tras perder la confirmación puede duplicar el evento. Notify guarda una fila con clave tópico + eventId para cada efecto. El listener de seguimiento registra etapas y el de auditoría incluye el actor. GET /api/notifications/kafka/{tramiteId} devuelve hasta cien registros a un administrador.

| Tópico | Grupo | Retención inicial |
|---|---|---|
| tramites-eventos | barrio-seguimiento | 7 días |
| tramites-auditoria | barrio-auditoria | 30 días |
| tramites-eventos-dlt y tramites-auditoria-dlt | Inspección operativa | 7 días |

Los dos tópicos funcionales y sus DLT se crean con tres particiones, tres réplicas, min.insync.replicas=2 y cleanup.policy=delete. La auditoría conserva una ventana más larga para revisar cambios. Los dos listeners validan el JSON. Los errores transitorios tienen dos reintentos después del intento inicial. Los registros inválidos se derivan directamente a la DLT. Notify solo publica en Kafka para recuperación técnica; el único productor de eventos de negocio y auditoría es requests.

Cuando un comando AMQP se rechaza definitivamente, notify primero aplica el `basicNack` hacia la DLQ de RabbitMQ y después copia de forma asíncrona y best-effort un sobre técnico al DLT Kafka del dominio correspondiente: auditoría (`q.cmd.2`) a `tramites-auditoria-dlt`; correo y seguimiento (`q.cmd.email`, `q.cmd.3`) a `tramites-eventos-dlt`. La copia se delega a un ejecutor acotado para que un Kafka degradado no bloquee el consumidor AMQP; si ese ejecutor se satura, el descarte queda registrado. El sobre incluye broker/cola de origen, routing key, `eventId`, `requestId`, `correlationId`, tipo de error, una vista UTF-8 del payload y `payloadBase64` con los bytes originales exactos. La DLQ AMQP sigue siendo la fuente de retención del comando rechazado si Kafka no está disponible; un fallo de copia queda registrado como error operativo. Así, la prueba de error controlado con Kafka saludable deja evidencia tanto en `q.cmd.email.dlq` como en `tramites-eventos-dlt`.

## Demostración de diez minutos
- Minutos 0–2. Mostrar el Compose activo, los nodos y la UI. Cada partición tiene un líder y dos réplicas adicionales; ISR identifica las copias sincronizadas. Todo corre en un host.
- Minutos 2–5. Ingresar un trámite como vecino y admitirlo como funcionario. Buscar el mismo tramiteId en ambos tópicos. Mostrar estadoAnterior, estado, actor y correlationId. Verificar el historial del consumidor y las tres colas de RabbitMQ.
- Minutos 5–7. Con rol Admin usar GET/POST /api/kafka/topics, GET /api/kafka/topics/{name}, GET/PUT /api/kafka/topics/{name}/config y DELETE /api/kafka/topics/{name}. Ejemplo de creación: {"name":"prueba-ep3","partitions":3,"replicas":3}. Para actualizar: {"retentionMs":3600000,"cleanupPolicy":"delete"}. Solo se permite borrar nombres prueba-*.
- Minutos 7–9. Ejecutar `sh scripts/kafka-grupos.sh` en Linux o usar Consumers en Kafka UI. Mostrar miembros y particiones asignadas. CURRENT-OFFSET es la siguiente posición confirmada; LOG-END-OFFSET es el final disponible y LAG es la diferencia. Crear el trámite con `error@barriodigital.test` y mostrar la evidencia en `tramites-eventos-dlt`; revisar también la DLT del ejercicio de recuperación local.
- Minuto 9–10. Explicar las confirmaciones, la deduplicación, la continuidad de EV2 y los límites de ejecutar todo en una sola instancia.

## Verificación desde fuentes
Ejecutar `./mvnw test` desde backend (Windows: `.\mvnw.cmd test`). La prueba scripts/smoke.py crea registros de prueba, verifica los dos tópicos, los cuatro registros del ingreso y la admisión, las configuraciones del administrador temporal, los flujos RabbitMQ y la copia del error controlado en `tramites-eventos-dlt`. La práctica de recuperación usa mensajes sintéticos identificables en el entorno demo. Los scripts requieren la pila en ejecución y acceso local a Docker Compose para leer la DLT.
