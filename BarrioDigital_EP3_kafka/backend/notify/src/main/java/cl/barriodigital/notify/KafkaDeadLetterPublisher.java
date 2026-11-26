package cl.barriodigital.notify;

import cl.barriodigital.common.CommandEvent;
import cl.barriodigital.common.MessagingNames;
import cl.barriodigital.common.TramiteEvent;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Base64;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class KafkaDeadLetterPublisher {
  private static final Logger log=LoggerFactory.getLogger(KafkaDeadLetterPublisher.class);
  private final KafkaTemplate<String,String> producer;
  private final ObjectMapper mapper;
  private final Executor copyExecutor;

  public KafkaDeadLetterPublisher(KafkaTemplate<String,String> producer,ObjectMapper mapper,
      @Qualifier("kafkaDeadLetterCopyExecutor") Executor copyExecutor) {
    this.producer=producer;
    this.mapper=mapper;
    this.copyExecutor=copyExecutor;
  }

  static String topicForQueue(String queue) {
    return MessagingNames.Q_AUDIT.equals(queue)?TramiteEvent.AUDITORIA_DLT:TramiteEvent.NEGOCIO_DLT;
  }

  public void publish(Message message,CommandEvent event,String queue,Exception cause) {
    byte[] body=message.getBody().clone();
    try {
      copyExecutor.execute(()->publishCopy(message,body,event,queue,cause));
    } catch(RejectedExecutionException ex) {
      log.error("Copia Kafka DLT descartada por saturación del ejecutor queue={} motivo={}",queue,ex.getMessage());
    }
  }

  private void publishCopy(Message message,byte[] body,CommandEvent event,String queue,Exception cause) {
    String topic=topicForQueue(queue);
    String key=null;
    try {
      var properties=message.getMessageProperties();
      String eventId=firstTextOrNull(event==null?null:event.eventId(),properties.getMessageId());
      String correlationId=firstTextOrNull(event==null?null:event.correlationId(),properties.getCorrelationId());
      String sourceId=firstTextOrNull(eventId,stablePayloadId(body,queue));
      key=firstTextOrNull(event==null?null:event.requestId(),sourceId);
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
      envelope.put("payload",new String(body,StandardCharsets.UTF_8));
      envelope.put("payloadBase64",Base64.getEncoder().encodeToString(body));

      String kafkaKey=key;
      producer.send(topic,kafkaKey,mapper.writeValueAsString(envelope)).whenComplete((result,error)->{
        if(error!=null) {
          log.error("No se pudo copiar comando rechazado a Kafka DLT topic={} key={} motivo={}",topic,kafkaKey,error.getMessage());
          return;
        }
        var metadata=result.getRecordMetadata();
        log.warn("Comando rechazado copiado a Kafka DLT topic={} key={} partition={} offset={}",topic,kafkaKey,metadata.partition(),metadata.offset());
      });
    } catch(Exception ex) {
      log.error("No se pudo iniciar copia de comando rechazado a Kafka DLT topic={} key={} motivo={}",topic,key,ex.getMessage());
    }
  }

  private static String stablePayloadId(byte[] body,String queue) {
    try {
      MessageDigest digest=MessageDigest.getInstance("SHA-256");
      digest.update((queue==null?"unknown":queue).getBytes(StandardCharsets.UTF_8));
      digest.update((byte)0);
      return HexFormat.of().formatHex(digest.digest(body));
    } catch(NoSuchAlgorithmException ex) {
      throw new IllegalStateException("SHA-256 no disponible",ex);
    }
  }

  private static String firstTextOrNull(String... values) {
    for(String value:values)if(value!=null&&!value.isBlank())return value;
    return null;
  }
}
