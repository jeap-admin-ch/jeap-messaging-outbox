package ch.admin.bit.jeap.messaging.transactionaloutbox.headers;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@RequiredArgsConstructor
class JdbcOutboxMessageHeadersRepository implements OutboxMessageHeadersRepository {

    private final JdbcTemplate jdbcTemplate;

    @PersistenceContext
    private EntityManager entityManager;

    @Override
    @Transactional(propagation = Propagation.MANDATORY)
    public void saveHeaders(long deferredMessageId, List<Header> headers) {
        // JPA may defer the parent INSERT until commit. The header table has a foreign key to it.
        entityManager.flush();
        for (int index = 0; index < headers.size(); index++) {
            Header header = headers.get(index);
            jdbcTemplate.update("""
                    INSERT INTO deferred_message_header (deferred_message_id, header_index, header_name, header_value)
                    VALUES (?, ?, ?, ?)
                    """, deferredMessageId, index, header.key(), header.value());
        }
    }

    @Override
    public List<Header> findHeaders(long deferredMessageId) {
        return jdbcTemplate.query("""
                SELECT header_name, header_value FROM deferred_message_header
                WHERE deferred_message_id = ? ORDER BY header_index
                """, (rs, _) -> new RecordHeader(rs.getString("header_name"), rs.getBytes("header_value")),
                deferredMessageId);
    }
}
