package ch.admin.bit.jeap.messaging.transactionaloutbox.messaging;

import ch.admin.bit.jeap.messaging.kafka.tracing.TraceContext;
import ch.admin.bit.jeap.messaging.kafka.tracing.TraceContextProvider;
import ch.admin.bit.jeap.messaging.kafka.tracing.TraceContextUpdater;
import ch.admin.bit.jeap.messaging.transactionaloutbox.outbox.OutboxTraceContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MessagingOutboxTracingTest {

    private static final Long TRACE_ID_HIGH = 1L;
    private static final Long TRACE_ID_LOW = 2L;
    private static final Long SPAN_ID = 3L;
    private static final Long PARENT_SPAN_ID = 4L;
    private static final String TRACE_ID_STRING = "00000000000000010000000000000002";

    @ParameterizedTest
    @CsvSource(value = {"true", "false", "null"}, nullValues = "null")
    void retrieveCurrentTraceContext_copiesAllFieldsFromTraceContext(Boolean sampled) {
        TraceContextProvider provider = mock(TraceContextProvider.class);
        when(provider.getTraceContext()).thenReturn(
                new TraceContext(TRACE_ID_HIGH, TRACE_ID_LOW, SPAN_ID, PARENT_SPAN_ID, TRACE_ID_STRING, sampled));
        MessagingOutboxTracing tracing = new MessagingOutboxTracing(Optional.of(provider), Optional.empty());

        OutboxTraceContext result = tracing.retrieveCurrentTraceContext();

        assertThat(result.getTraceIdHigh()).isEqualTo(TRACE_ID_HIGH);
        assertThat(result.getTraceId()).isEqualTo(TRACE_ID_LOW);
        assertThat(result.getSpanId()).isEqualTo(SPAN_ID);
        assertThat(result.getParentSpanId()).isEqualTo(PARENT_SPAN_ID);
        assertThat(result.getTraceIdString()).isEqualTo(TRACE_ID_STRING);
        assertThat(result.getSampled()).isEqualTo(sampled);
    }


    @ParameterizedTest
    @CsvSource(value = {"true", "false", "null"}, nullValues = "null")
    @SuppressWarnings("resource")
    void updateCurrentTraceContext_forwardsAllFieldsToUpdater(Boolean sampled) {
        TraceContextUpdater updater = mock(TraceContextUpdater.class);
        when(updater.setTraceContext(any())).thenReturn(() -> { });
        MessagingOutboxTracing tracing = new MessagingOutboxTracing(Optional.empty(), Optional.of(updater));
        OutboxTraceContext persisted = OutboxTraceContext.builder()
                .traceIdHigh(TRACE_ID_HIGH)
                .traceId(TRACE_ID_LOW)
                .spanId(SPAN_ID)
                .parentSpanId(PARENT_SPAN_ID)
                .traceIdString(TRACE_ID_STRING)
                .sampled(sampled)
                .build();

        tracing.updateCurrentTraceContext(persisted);

        ArgumentCaptor<TraceContext> captor = ArgumentCaptor.forClass(TraceContext.class);
        verify(updater).setTraceContext(captor.capture());
        TraceContext forwarded = captor.getValue();
        assertThat(forwarded.getTraceIdHigh()).isEqualTo(TRACE_ID_HIGH);
        assertThat(forwarded.getTraceId()).isEqualTo(TRACE_ID_LOW);
        assertThat(forwarded.getSpanId()).isEqualTo(SPAN_ID);
        assertThat(forwarded.getParentSpanId()).isEqualTo(PARENT_SPAN_ID);
        assertThat(forwarded.getTraceIdString()).isEqualTo(TRACE_ID_STRING);
        assertThat(forwarded.getSampled()).isEqualTo(sampled);
    }
}
