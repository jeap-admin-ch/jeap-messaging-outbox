package ch.admin.bit.jeap.messaging.transactionaloutbox.outbox;

import ch.admin.bit.jeap.messaging.kafka.tracing.TraceContextScope;

public interface OutboxTracing {

    OutboxTraceContext retrieveCurrentTraceContext();

    /**
     * Activates the given trace context as the current tracing context on this thread. The returned
     * {@link TraceContextScope} must be closed (preferably via try-with-resources) to restore the previously-active
     * context. If the argument is {@code null} or no tracer is available, a no-op scope is returned.
     */
    TraceContextScope updateCurrentTraceContext(OutboxTraceContext outboxTraceContext);

}
