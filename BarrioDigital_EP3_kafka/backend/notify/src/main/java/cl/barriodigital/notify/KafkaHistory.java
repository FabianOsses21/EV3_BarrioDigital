package cl.barriodigital.notify;

import cl.barriodigital.common.TramiteEvent;
import jakarta.persistence.*;
import java.time.Instant;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.*;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import tools.jackson.databind.ObjectMapper;

@Entity
@Table(name="kafka_history")
class HistoryEntry {
    @Id public String id;
    public String topic;
    public String tramiteId;
    public String eventType;
    public String actor;
    public String estadoAnterior;
    public String estado;
    public String correlationId;
    public String occurredAt;
}

interface HistoryRepository extends JpaRepository<HistoryEntry,String> {
    List<HistoryEntry> findTop100ByTramiteIdOrderByOccurredAtDesc(String tramiteId);
}

@Service
class KafkaHistory {
    private static final Logger log = LoggerFactory.getLogger(KafkaHistory.class);
    private final ObjectMapper mapper;
    private final HistoryRepository history;

    KafkaHistory(ObjectMapper mapper, HistoryRepository history) {
        this.mapper = mapper;
        this.history = history;
    }

    @KafkaListener(topics=TramiteEvent.NEGOCIO, groupId="barrio-seguimiento")
    @KafkaListener(topics=TramiteEvent.AUDITORIA, groupId="barrio-auditoria")
    @Transactional
    public void process(ConsumerRecord<String,String> record) {
        TramiteEvent event;
        try {
            event = mapper.readValue(record.value(), TramiteEvent.class);
            event.validate(record.key());
        } catch (RuntimeException ex) {
            throw new IllegalArgumentException("Evento inválido en " + record.topic(), ex);
        }
        String id = record.topic() + "|" + event.eventId();
        if (history.existsById(id)) return;
        var row = new HistoryEntry();
        row.id = id;
        row.topic = record.topic();
        row.tramiteId = record.key();
        row.eventType = event.eventType();
        row.occurredAt = event.occurredAt().toString();
        row.estado = event.estado();
        row.estadoAnterior = event.estadoAnterior();
        if (TramiteEvent.AUDITORIA.equals(record.topic())) row.actor = event.actor();
        row.correlationId = event.correlationId();
        history.saveAndFlush(row);
        log.info("Procesado topic={} eventId={} type={} partition={} offset={}", record.topic(), event.eventId(),
                event.eventType(), record.partition(), record.offset());
    }
}

@RestController
@RequestMapping("/api/notifications/kafka")
@PreAuthorize("hasRole('ADMIN')")
class KafkaHistoryController {
    private final HistoryRepository history;
    KafkaHistoryController(HistoryRepository history) { this.history = history; }

    @GetMapping("/{tramiteId}")
    public List<HistoryEntry> history(@PathVariable String tramiteId) {
        return history.findTop100ByTramiteIdOrderByOccurredAtDesc(tramiteId);
    }
}
