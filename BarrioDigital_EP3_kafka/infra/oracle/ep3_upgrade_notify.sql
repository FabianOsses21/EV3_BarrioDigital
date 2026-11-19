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
