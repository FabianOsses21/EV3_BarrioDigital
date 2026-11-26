package cl.barriodigital.notify;

import cl.barriodigital.common.TramiteEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.*;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
class KafkaRecovery {
    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String,String> template) {
        var recoverer = new DeadLetterPublishingRecoverer(template,
                (record, error) -> dltDestination(record));
        recoverer.setFailIfSendResultIsError(true);
        var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2));
        handler.addNotRetryableExceptions(IllegalArgumentException.class);
        return handler;
    }

    static TopicPartition dltDestination(ConsumerRecord<?,?> record) {
        String topic;
        if (TramiteEvent.NEGOCIO.equals(record.topic())) topic = TramiteEvent.NEGOCIO_DLT;
        else if (TramiteEvent.AUDITORIA.equals(record.topic())) topic = TramiteEvent.AUDITORIA_DLT;
        else topic = record.topic() + "-dlt";
        return new TopicPartition(topic, record.partition());
    }
}
