package ch.admin.bit.jeap.messaging.transactionaloutbox.headers;

import org.apache.kafka.common.header.Header;

import java.util.List;

/**
 * Optional durable storage for outbox headers. Implementations must use the same database and
 * transaction as the outbox and retain headers until their deferred message is deleted.
 */
public interface OutboxMessageHeadersRepository {

    /** Save an ordered list of headers in the transaction that inserts the deferred message. */
    void saveHeaders(long deferredMessageId, List<Header> headers);

    /** Load headers for delivery, returning an empty list for messages without headers. */
    List<Header> findHeaders(long deferredMessageId);
}
