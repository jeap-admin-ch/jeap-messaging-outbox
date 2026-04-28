package ch.admin.bit.jeap.messaging.transactionaloutbox.messaging;

import ch.admin.bit.jeap.messaging.kafka.tracing.TraceContext;
import ch.admin.bit.jeap.messaging.kafka.tracing.TraceContextProvider;
import ch.admin.bit.jeap.messaging.kafka.tracing.TraceContextScope;
import ch.admin.bit.jeap.messaging.kafka.tracing.TraceContextUpdater;
import ch.admin.bit.jeap.messaging.transactionaloutbox.outbox.OutboxTraceContext;
import ch.admin.bit.jeap.messaging.transactionaloutbox.outbox.OutboxTracing;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Slf4j
@RequiredArgsConstructor
@Component
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class MessagingOutboxTracing implements OutboxTracing {

    private static final TraceContextScope NOOP_SCOPE = () -> { };

    private final Optional<TraceContextProvider> traceContextProvider;

    private final Optional<TraceContextUpdater> traceContextUpdater;

    @Override
    public OutboxTraceContext retrieveCurrentTraceContext() {
        if (traceContextProvider.isEmpty()) {
            log.debug("No tracing information available (no trace context provider present).");
            return null;
        }
        TraceContext traceContext = traceContextProvider.get().getTraceContext();
        if (traceContext == null) {
            log.debug("No tracing information available (trace context is null).");
            return null;
        }
        OutboxTraceContext outboxTraceContext = OutboxTraceContext.builder()
                .traceIdHigh(traceContext.getTraceIdHigh())
                .traceId(traceContext.getTraceId())
                .spanId(traceContext.getSpanId())
                .parentSpanId(traceContext.getParentSpanId())
                .traceIdString(traceContext.getTraceIdString())
                .sampled(traceContext.getSampled())
                .build();
        log.debug("Tracing information (traceId={}) found in context. Returning trace context {}",
                outboxTraceContext.getTraceIdString(), outboxTraceContext);
        return outboxTraceContext;
    }

    @Override
    public TraceContextScope updateCurrentTraceContext(OutboxTraceContext outboxTraceContext) {
        if (traceContextUpdater.isEmpty() || outboxTraceContext == null) {
            return NOOP_SCOPE;
        }
        log.debug("Original trace context found on the message to send (traceId={}). Overriding the current tracing context with {}",
                outboxTraceContext.getTraceIdString(), outboxTraceContext);
        TraceContext target = new TraceContext(
                outboxTraceContext.getTraceIdHigh(),
                outboxTraceContext.getTraceId(),
                outboxTraceContext.getSpanId(),
                outboxTraceContext.getParentSpanId(),
                outboxTraceContext.getTraceIdString(),
                outboxTraceContext.getSampled());
        return traceContextUpdater.get().setTraceContext(target);
    }
}
