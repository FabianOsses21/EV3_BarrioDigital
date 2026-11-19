-- Esquema NOTIFY (bd_notify).

CREATE TABLE notification_receipts (
    id             VARCHAR2(150 CHAR) NOT NULL,
    event_id       VARCHAR2(64 CHAR),
    request_id     VARCHAR2(64 CHAR),
    correlation_id VARCHAR2(100 CHAR),
    queue_name     VARCHAR2(80 CHAR),
    recipient      VARCHAR2(150 CHAR),
    result         VARCHAR2(40 CHAR),
    processed_at   TIMESTAMP,
    CONSTRAINT pk_notification_receipts PRIMARY KEY (id)
);

CREATE TABLE kafka_history (
    id VARCHAR2(255 CHAR) PRIMARY KEY,
    topic VARCHAR2(255 CHAR),
    tramite_id VARCHAR2(255 CHAR),
    event_type VARCHAR2(255 CHAR),
    actor VARCHAR2(255 CHAR),
    estado_anterior VARCHAR2(255 CHAR),
    estado VARCHAR2(255 CHAR),
    correlation_id VARCHAR2(255 CHAR),
    occurred_at VARCHAR2(255 CHAR)
);
