package cl.barriodigital.requests;
import java.time.LocalDateTime;
import java.util.List;
import org.springframework.data.jpa.repository.*;
public interface OutboxRepository extends JpaRepository<OutboxMessage,String> {
  List<OutboxMessage> findTop20BySentAtIsNullAndExchangeNameNotOrderByCreatedAtAsc(String excluded);
  List<OutboxMessage> findTop20BySentAtIsNullAndExchangeNameOrderByCreatedAtAsc(String exchange);
  long countBySentAtIsNull();
  long deleteBySentAtBefore(LocalDateTime cutoff);
}
