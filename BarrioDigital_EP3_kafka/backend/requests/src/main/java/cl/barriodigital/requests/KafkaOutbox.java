package cl.barriodigital.requests;

import cl.barriodigital.common.*;
import cl.barriodigital.requests.entity.Tramite;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.slf4j.*;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
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

    KafkaDispatcher(KafkaTemplate<String,String> producer, OutboxRepository repository, ObjectMapper mapper) {
        this.producer = producer;
        this.repository = repository;
        this.mapper = mapper;
    }

    @Scheduled(fixedDelay=1500)
    public void dispatch() {
        for (var row : repository.findTop20BySentAtIsNullAndExchangeNameOrderByCreatedAtAsc(KafkaOutbox.DESTINATION)) {
            if (row.nextAttempt.isAfter(LocalDateTime.now())) break;
            try {
                var event = mapper.readValue(row.payload, TramiteEvent.class);
                var result = producer.send(row.routingKey, event.tramiteId().toString(), row.payload).get(12, TimeUnit.SECONDS);
                log.info("Publicado topic={} eventId={} partition={} offset={}", row.routingKey, row.id,
                        result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
                row.sentAt = LocalDateTime.now();
                row.lastError = null;
            } catch (Exception ex) {
                if (ex instanceof InterruptedException) Thread.currentThread().interrupt();
                row.lastError = ex.getClass().getSimpleName();
                row.nextAttempt = LocalDateTime.now().plusSeconds(Math.min(60, 1L << Math.min(6, row.attempts)));
                log.warn("Kafka pendiente topic={} eventId={} error={}", row.routingKey, row.id, row.lastError);
            }
            row.attempts++;
            repository.save(row);
            if (row.sentAt == null) break;
        }
    }
}
