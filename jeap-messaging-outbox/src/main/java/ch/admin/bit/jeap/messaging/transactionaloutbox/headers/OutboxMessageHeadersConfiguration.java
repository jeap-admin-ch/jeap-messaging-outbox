package ch.admin.bit.jeap.messaging.transactionaloutbox.headers;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;

/** Opt-in JDBC storage; no additional JPA mappings or schema requirements when disabled. */
@AutoConfiguration
@ConditionalOnProperty(prefix = "jeap.messaging.transactional-outbox", name = "headers-enabled", havingValue = "true")
public class OutboxMessageHeadersConfiguration {

    @Bean
    @ConditionalOnMissingBean(OutboxMessageHeadersRepository.class)
    OutboxMessageHeadersRepository outboxMessageHeadersRepository(JdbcTemplate jdbcTemplate) {
        return new JdbcOutboxMessageHeadersRepository(jdbcTemplate);
    }
}
