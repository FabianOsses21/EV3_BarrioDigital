package cl.barriodigital.requests;

import cl.barriodigital.common.*;
import cl.barriodigital.requests.entity.Tramite;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.slf4j.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.Pageable;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class KafkaOutbox {
    static final String DESTINATION = "kafka";
    private final OutboxRepository repository;
    private final ObjectMapper mapper;
    private final Identity identity;

    public KafkaOutbox(OutboxRepository repository, ObjectMapper mapper, Identity identity) {
        this.repository = repository;
        this.mapper = mapper;
        this.identity = identity;
    }

    public void record(Tramite tramite, String previous) {
        String correlation = Optional.ofNullable(MDC.get("correlationId")).orElseGet(() -> UUID.randomUUID().toString());
        for (String topic : List.of(TramiteEvent.NEGOCIO, TramiteEvent.AUDITORIA)) {
            var event = new TramiteEvent(UUID.randomUUID(), UUID.fromString(tramite.getId()),
                    previous == null ? "TRAMITE_INGRESADO" : "ETAPA_CAMBIADA", Instant.now(),
                    previous, tramite.getEstado().name(), identity.id(), correlation);
            repository.save(new OutboxMessage(event.eventId().toString(), DESTINATION, topic, correlation,
                    mapper.writeValueAsString(event)));
        }
    }
}

@Component
@ConditionalOnProperty(name="app.outbox.enabled", havingValue="true", matchIfMissing=true)
class KafkaDispatcher {
    private static final Logger log = LoggerFactory.getLogger(KafkaDispatcher.class);
    private final KafkaTemplate<String,String> producer;
    private final OutboxRepository repository;
    private final ObjectMapper mapper;

    private static final int MAX_ATTEMPTS = 25;

    KafkaDispatcher(KafkaTemplate<String,String> producer, OutboxRepository repository, ObjectMapper mapper) {
        this.producer = producer;
        this.repository = repository;
        this.mapper = mapper;
    }

    @Scheduled(fixedDelay=1500)
    public void dispatch() {
        var now = LocalDateTime.now();
        var blocked = new HashSet<String>();
        for (var row : repository.findPendingKafka(KafkaOutbox.DESTINATION, Pageable.ofSize(20))) {
            TramiteEvent event;
            try {
                event = mapper.readValue(row.payload, TramiteEvent.class);
            } catch (Exception ex) {
                row.attempts++;
                row.failed = true;
                row.lastError = ex.getClass().getSimpleName();
                log.error("Evento Kafka descartado por payload inválido topic={} eventId={} error={}",
                        row.routingKey, row.id, row.lastError);
                repository.save(row);
                continue;
            }
            var key = row.routingKey + '|' + event.tramiteId();
            if (blocked.contains(key)) continue;
            if (row.nextAttempt.isAfter(now)) { blocked.add(key); continue; }
            row.attempts++;
            try {
                var result = producer.send(row.routingKey, event.tramiteId().toString(), row.payload).get(12, TimeUnit.SECONDS);
                log.info("Publicado topic={} eventId={} partition={} offset={}", row.routingKey, row.id,
                        result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
                row.sentAt = LocalDateTime.now();
                row.lastError = null;
            } catch (Exception ex) {
                if (ex instanceof InterruptedException) Thread.currentThread().interrupt();
                row.lastError = ex.getClass().getSimpleName();
                if (row.attempts >= MAX_ATTEMPTS) {
                    row.failed = true;
                    log.error("Evento Kafka descartado tras {} intentos topic={} eventId={} error={}",
                            row.attempts, row.routingKey, row.id, row.lastError);
                } else {
                    row.nextAttempt = LocalDateTime.now().plusSeconds(Math.min(60, 1L << Math.min(6, row.attempts)));
                    log.warn("Kafka pendiente topic={} eventId={} error={}", row.routingKey, row.id, row.lastError);
                }
                blocked.add(key);
            }
            repository.save(row);
        }
    }
}
