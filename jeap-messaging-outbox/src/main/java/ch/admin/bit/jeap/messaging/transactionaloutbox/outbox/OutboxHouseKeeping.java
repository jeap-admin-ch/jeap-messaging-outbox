package ch.admin.bit.jeap.messaging.transactionaloutbox.outbox;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.ZonedDateTime;
import java.util.Set;
import java.util.function.Supplier;

@Slf4j
@Component
public class OutboxHouseKeeping {

    private final DeferredMessageRepository deferredMessageRepository;
    private final TransactionalOutboxConfiguration config;
    private final TransactionTemplate transactionTemplate;
    private final Pageable pageable;

    public OutboxHouseKeeping(DeferredMessageRepository deferredMessageRepository, TransactionalOutboxConfiguration config, TransactionTemplate transactionTemplate) {
        this.deferredMessageRepository = deferredMessageRepository;
        this.config = config;
        this.transactionTemplate = transactionTemplate;
        this.pageable = Pageable.ofSize(config.getHouseKeepingPageSize());
    }

    public void deleteOldMessages() {
        log.info("Housekeeping: deleting old messages with page size {} and max pages {}", config.getHouseKeepingPageSize(), config.getHouseKeepingMaxPages());
        ZonedDateTime now = ZonedDateTime.now();
        deleteMessagesSentBefore(now.minus(config.getSentMessageRetentionDuration()));
        deleteUnsentMessagesCreatedBefore(now.minus(config.getUnsentMessageRetentionDuration()));
        log.info("Housekeeping: deleted sent messages and unsent messages done");
    }

    private void deleteMessagesSentBefore(ZonedDateTime olderThan) {
        log.info("Housekeeping: deleting messages sent before {}", olderThan);
        executeInTransactionPerPage(() -> deleteMessagesSentBeforeInPage(olderThan));
    }

    private void deleteUnsentMessagesCreatedBefore(ZonedDateTime olderThan) {
        log.info("Housekeeping: deleting unsent messages created before {}", olderThan);
        executeInTransactionPerPage(() -> deleteUnsentMessagesCreatedBeforeInPage(olderThan));
    }

    private boolean deleteMessagesSentBeforeInPage(ZonedDateTime olderThan) {
        final Slice<Long> resultPage = deferredMessageRepository.findSentImmediatelyBeforeOrSentScheduledBefore(olderThan, pageable);
        final Set<Long> messageIds = resultPage.toSet();
        if (!messageIds.isEmpty()) {
            deferredMessageRepository.deleteAllById(messageIds);
        }
        log.info("Housekeeping: deleted {} sent messages", messageIds.size());
        return resultPage.hasNext();
    }

    private boolean deleteUnsentMessagesCreatedBeforeInPage(ZonedDateTime olderThan) {
        final Slice<Long> resultPage = deferredMessageRepository.findSentImmediatelyIsNullAndSentScheduledIsNullAndCreatedBefore(olderThan, pageable);
        final Set<Long> messageIds = resultPage.toSet();
        if (!messageIds.isEmpty()) {
            deferredMessageRepository.deleteAllById(messageIds);
        }
        log.info("Housekeeping: deleted {} unsent messages", messageIds.size());
        return resultPage.hasNext();
    }

    /**
     * A hibernate session flush is forced after every page by using a new transaction. This also reduces
     * transaction size and minimizes the duration of locks during housekeeping.
     */
    @SuppressWarnings({"java:S4276", "java:S2259"})
    private void executeInTransactionPerPage(Supplier<Boolean> callback) {
        int pages = 0;
        while (pages < config.getHouseKeepingMaxPages()) {
            boolean hasMorePages = transactionTemplate.execute(status -> callback.get());
            if (!hasMorePages) {
                break;
            }
            pages++;
        }
    }


}
