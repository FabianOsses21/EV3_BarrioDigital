package cl.barriodigital.requests;
import java.time.LocalDateTime;
import java.util.List;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;
public interface OutboxRepository extends JpaRepository<OutboxMessage,String> {
  List<OutboxMessage> findTop20BySentAtIsNullAndExchangeNameNotOrderByCreatedAtAsc(String excluded);
  @Query("select m from OutboxMessage m where m.sentAt is null and m.exchangeName = :exchange"
      + " and (m.failed = false or m.failed is null) order by m.createdAt asc")
  List<OutboxMessage> findPendingKafka(@Param("exchange") String exchange, Pageable pageable);
  @Query("select count(m) from OutboxMessage m where m.sentAt is null and (m.failed = false or m.failed is null)")
  long countPending();
  long countByFailedTrue();
  long deleteBySentAtBefore(LocalDateTime cutoff);
}
