package cl.barriodigital.notify;

import cl.barriodigital.common.CommandEvent;
import cl.barriodigital.common.MessagingNames;
import cl.barriodigital.common.TramiteEvent;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class KafkaDeadLetterPublisher {
  private static final Logger log=LoggerFactory.getLogger(KafkaDeadLetterPublisher.class);
  private final KafkaTemplate<String,String> producer;
  private final ObjectMapper mapper;

  public KafkaDeadLetterPublisher(KafkaTemplate<String,String> producer,ObjectMapper mapper) {
    this.producer=producer;
    this.mapper=mapper;
  }

  static String topicForQueue(String queue) {
    return MessagingNames.Q_AUDIT.equals(queue)?TramiteEvent.AUDITORIA_DLT:TramiteEvent.NEGOCIO_DLT;
  }

  public void publish(Message message,CommandEvent event,String queue,Exception cause) {
    var properties=message.getMessageProperties();
    String eventId=firstTextOrNull(event==null?null:event.eventId(),properties.getMessageId());
    String correlationId=firstTextOrNull(event==null?null:event.correlationId(),properties.getCorrelationId());
    String key=firstText(event==null?null:event.requestId(),eventId,properties.getMessageId());
    String sourceId=firstText(eventId,properties.getMessageId());
    String deadLetterId=sourceId+"|"+(queue==null?"unknown":queue);
    var envelope=new LinkedHashMap<String,Object>();
    envelope.put("deadLetterId",deadLetterId);
    envelope.put("sourceBroker","rabbitmq");
    envelope.put("sourceQueue",queue);
    envelope.put("sourceRoutingKey",properties.getReceivedRoutingKey());
    envelope.put("eventId",eventId);
    envelope.put("requestId",event==null?null:event.requestId());
    envelope.put("correlationId",correlationId);
    envelope.put("errorType",cause.getClass().getSimpleName());
    envelope.put("errorMessage",cause.getMessage());
    envelope.put("failedAt",Instant.now().toString());
    envelope.put("payload",new String(message.getBody(),StandardCharsets.UTF_8));
    String topic=topicForQueue(queue);
    try {
      producer.send(topic,key,mapper.writeValueAsString(envelope)).whenComplete((result,error)->{
        if(error!=null) {
          log.error("No se pudo copiar comando rechazado a Kafka DLT topic={} key={} motivo={}",topic,key,error.getMessage());
          return;
        }
        var metadata=result.getRecordMetadata();
        log.warn("Comando rechazado copiado a Kafka DLT topic={} key={} partition={} offset={}",topic,key,metadata.partition(),metadata.offset());
      });
    } catch(Exception ex) {
      log.error("No se pudo iniciar copia de comando rechazado a Kafka DLT topic={} key={} motivo={}",topic,key,ex.getMessage());
    }
  }

  private static String firstText(String... values) {
    String value=firstTextOrNull(values);
    return value==null?UUID.randomUUID().toString():value;
  }

  private static String firstTextOrNull(String... values) {
    for(String value:values)if(value!=null&&!value.isBlank())return value;
    return null;
  }
}
