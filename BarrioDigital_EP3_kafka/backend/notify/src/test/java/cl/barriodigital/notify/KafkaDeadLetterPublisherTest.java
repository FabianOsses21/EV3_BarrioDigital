package cl.barriodigital.notify;

import cl.barriodigital.common.CommandEvent;
import cl.barriodigital.common.MessagingNames;
import cl.barriodigital.common.TramiteEvent;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import tools.jackson.databind.json.JsonMapper;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaDeadLetterPublisherTest {
  KafkaTemplate<String,String> producer;
  JsonMapper mapper;
  KafkaDeadLetterPublisher publisher;

  @BeforeEach @SuppressWarnings("unchecked") void setup() {
    producer=mock(KafkaTemplate.class);
    mapper=JsonMapper.builder().build();
    publisher=new KafkaDeadLetterPublisher(producer,mapper);
  }

  @Test void auditCommandsUseAuditDltAndOperationalCommandsUseBusinessDlt() {
    assertThat(KafkaDeadLetterPublisher.topicForQueue(MessagingNames.Q_AUDIT)).isEqualTo(TramiteEvent.AUDITORIA_DLT);
    assertThat(KafkaDeadLetterPublisher.topicForQueue(MessagingNames.Q_EMAIL)).isEqualTo(TramiteEvent.NEGOCIO_DLT);
    assertThat(KafkaDeadLetterPublisher.topicForQueue(MessagingNames.Q_TRACKING)).isEqualTo(TramiteEvent.NEGOCIO_DLT);
  }

  @Test void terminalFailurePublishesStructuredEnvelopeToKafkaDlt() {
    CommandEvent event=new CommandEvent("event-1","corr-1","creado","2026-10-17T00:00:00Z","request-1","owner-1","error@barriodigital.test","Prueba","INGRESADO");
    var properties=new MessageProperties();
    properties.setConsumerQueue(MessagingNames.Q_EMAIL);
    properties.setReceivedRoutingKey(MessagingNames.RK_EMAIL);
    properties.setMessageId("event-1");
    properties.setCorrelationId("corr-1");
    Message message=new Message("{\"eventId\":\"event-1\"}".getBytes(),properties);
    when(producer.send(eq(TramiteEvent.NEGOCIO_DLT),eq("request-1"),anyString())).thenReturn(sendResult(TramiteEvent.NEGOCIO_DLT));

    publisher.publish(message,event,MessagingNames.Q_EMAIL,new IllegalStateException("Error controlado"));

    ArgumentCaptor<String> payload=ArgumentCaptor.forClass(String.class);
    verify(producer).send(eq(TramiteEvent.NEGOCIO_DLT),eq("request-1"),payload.capture());
    Map<String,Object> envelope=mapper.readValue(payload.getValue(),Map.class);
    assertThat(envelope).containsEntry("sourceBroker","rabbitmq")
      .containsEntry("deadLetterId","event-1|"+MessagingNames.Q_EMAIL)
      .containsEntry("sourceQueue",MessagingNames.Q_EMAIL)
      .containsEntry("sourceRoutingKey",MessagingNames.RK_EMAIL)
      .containsEntry("eventId","event-1")
      .containsEntry("requestId","request-1")
      .containsEntry("correlationId","corr-1")
      .containsEntry("errorType","IllegalStateException")
      .containsEntry("errorMessage","Error controlado");
    assertThat(envelope.get("payload")).asString().contains("event-1");
  }

  @Test void malformedCommandFallsBackToMessageIdAsKafkaKey() {
    var properties=new MessageProperties();
    properties.setConsumerQueue(MessagingNames.Q_AUDIT);
    properties.setMessageId("amqp-message-1");
    Message message=new Message("{invalid".getBytes(),properties);
    when(producer.send(eq(TramiteEvent.AUDITORIA_DLT),eq("amqp-message-1"),anyString())).thenReturn(sendResult(TramiteEvent.AUDITORIA_DLT));

    publisher.publish(message,null,MessagingNames.Q_AUDIT,new IllegalArgumentException("JSON inválido"));

    verify(producer).send(eq(TramiteEvent.AUDITORIA_DLT),eq("amqp-message-1"),anyString());
  }

  @Test void kafkaFailureDoesNotEscapeAndBlockRabbitRejection() {
    var properties=new MessageProperties();
    properties.setConsumerQueue(MessagingNames.Q_EMAIL);
    properties.setMessageId("event-1");
    Message message=new Message("{}".getBytes(),properties);
    when(producer.send(anyString(),anyString(),anyString())).thenReturn(CompletableFuture.failedFuture(new RuntimeException("broker down")));

    assertThatCode(()->publisher.publish(message,null,MessagingNames.Q_EMAIL,new IllegalStateException("fallo"))).doesNotThrowAnyException();
  }

  private CompletableFuture<SendResult<String,String>> sendResult(String topic) {
    RecordMetadata metadata=new RecordMetadata(new TopicPartition(topic,0),0,0,0,0,0);
    return CompletableFuture.completedFuture(new SendResult<>(new ProducerRecord<>(topic,"key","{}"),metadata));
  }
}
