package cl.barriodigital.notify;

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
                (record, error) -> new TopicPartition(record.topic() + "-dlt", record.partition()));
        recoverer.setFailIfSendResultIsError(true);
        var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2));
        handler.addNotRetryableExceptions(IllegalArgumentException.class);
        return handler;
    }
}
