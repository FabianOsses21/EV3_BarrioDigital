package cl.barriodigital.notify;

import cl.barriodigital.common.TramiteEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class KafkaRecoveryTest {
  @Test void businessRecordUsesBusinessDltTopicAndSamePartition() {
    var destination=KafkaRecovery.dltDestination(new ConsumerRecord<>(TramiteEvent.NEGOCIO,2,0,"key","{}"));
    assertThat(destination.topic()).isEqualTo(TramiteEvent.NEGOCIO_DLT);
    assertThat(destination.partition()).isEqualTo(2);
  }

  @Test void auditRecordUsesAuditDltTopicAndSamePartition() {
    var destination=KafkaRecovery.dltDestination(new ConsumerRecord<>(TramiteEvent.AUDITORIA,1,0,"key","{}"));
    assertThat(destination.topic()).isEqualTo(TramiteEvent.AUDITORIA_DLT);
    assertThat(destination.partition()).isEqualTo(1);
  }
}
