package cl.barriodigital.requests;

import cl.barriodigital.common.TramiteEvent;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.*;
import org.springframework.data.domain.Pageable;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import tools.jackson.databind.ObjectMapper;
import static org.mockito.Mockito.*;
import static org.assertj.core.api.Assertions.*;

class KafkaDispatcherTest {
  OutboxRepository repo; KafkaTemplate<String,String> producer; KafkaDispatcher dispatcher;
  ObjectMapper mapper=new ObjectMapper();
  String tramiteA=UUID.randomUUID().toString(), tramiteB=UUID.randomUUID().toString();

  @BeforeEach void setup() {
    repo=mock(OutboxRepository.class);producer=mock(KafkaTemplate.class);
    dispatcher=new KafkaDispatcher(producer,repo,mapper);
  }

  OutboxMessage row(String id,String tramite) {
    var event=new TramiteEvent(UUID.randomUUID(),UUID.fromString(tramite),"TRAMITE_INGRESADO",
        Instant.now(),null,"INGRESADO","tester","corr-"+id);
    return new OutboxMessage(id,KafkaOutbox.DESTINATION,TramiteEvent.NEGOCIO,"corr-"+id,
        mapper.writeValueAsString(event));
  }

  void rows(OutboxMessage... pending) {
    when(repo.findPendingKafka(eq(KafkaOutbox.DESTINATION),any(Pageable.class))).thenReturn(List.of(pending));
  }

  CompletableFuture<SendResult<String,String>> ok() {
    RecordMetadata meta=new RecordMetadata(new TopicPartition(TramiteEvent.NEGOCIO,0),0,0,0,0,0);
    SendResult<String,String> result=new SendResult<>(new ProducerRecord<>(TramiteEvent.NEGOCIO,"k","{}"),meta);
    return CompletableFuture.completedFuture(result);
  }

  CompletableFuture<SendResult<String,String>> fail() {
    return CompletableFuture.failedFuture(new RuntimeException("broker down"));
  }

  @Test void failureDoesNotBlockOtherKeys() {
    OutboxMessage a1=row("a1",tramiteA),b1=row("b1",tramiteB);rows(a1,b1);
    when(producer.send(anyString(),anyString(),anyString())).thenReturn(fail()).thenReturn(ok());
    dispatcher.dispatch();
    assertThat(a1.sentAt).isNull();assertThat(a1.nextAttempt).isAfter(LocalDateTime.now());
    assertThat(b1.sentAt).isNotNull();
    verify(producer,times(2)).send(anyString(),anyString(),anyString());
  }

  @Test void failurePreservesSameKeyOrdering() {
    OutboxMessage a1=row("a1",tramiteA),a2=row("a2",tramiteA),b1=row("b1",tramiteB);rows(a1,a2,b1);
    when(producer.send(anyString(),anyString(),anyString())).thenReturn(fail()).thenReturn(ok());
    dispatcher.dispatch();
    assertThat(a1.sentAt).isNull();assertThat(a2.sentAt).isNull();assertThat(a2.attempts).isZero();
    assertThat(b1.sentAt).isNotNull();
    verify(producer,times(2)).send(anyString(),anyString(),anyString());
    verify(repo,never()).save(a2);
  }

  @Test void consecutiveSameKeyRowsFlowWhilePreviousSucceeds() {
    OutboxMessage a1=row("a1",tramiteA),a2=row("a2",tramiteA);rows(a1,a2);
    when(producer.send(anyString(),anyString(),anyString())).thenReturn(ok());
    dispatcher.dispatch();
    assertThat(a1.sentAt).isNotNull();assertThat(a2.sentAt).isNotNull();
    verify(producer,times(2)).send(anyString(),anyString(),anyString());
  }

  @Test void notDueRowDefersOnlyItsKey() {
    OutboxMessage a1=row("a1",tramiteA),a2=row("a2",tramiteA),b1=row("b1",tramiteB);rows(a1,a2,b1);
    a1.nextAttempt=LocalDateTime.now().plusMinutes(5);
    when(producer.send(anyString(),anyString(),anyString())).thenReturn(ok());
    dispatcher.dispatch();
    assertThat(a1.sentAt).isNull();assertThat(a2.sentAt).isNull();assertThat(a2.attempts).isZero();
    assertThat(b1.sentAt).isNotNull();
    verify(producer,times(1)).send(anyString(),anyString(),anyString());
  }

  @Test void poisonPayloadMarkedFailedWithoutBlockingOthers() {
    OutboxMessage bad=new OutboxMessage("bad",KafkaOutbox.DESTINATION,TramiteEvent.NEGOCIO,"corr-bad","{invalid");
    OutboxMessage b1=row("b1",tramiteB);rows(bad,b1);
    when(producer.send(anyString(),anyString(),anyString())).thenReturn(ok());
    dispatcher.dispatch();
    assertThat(bad.failed).isTrue();assertThat(bad.sentAt).isNull();
    assertThat(b1.sentAt).isNotNull();
    verify(repo).save(bad);
  }

  @Test void exhaustedRetriesMarkRowFailed() {
    OutboxMessage a1=row("a1",tramiteA);a1.attempts=24;rows(a1);
    when(producer.send(anyString(),anyString(),anyString())).thenReturn(fail());
    dispatcher.dispatch();
    assertThat(a1.attempts).isEqualTo(25);
    assertThat(a1.failed).isTrue();assertThat(a1.sentAt).isNull();
  }
}
