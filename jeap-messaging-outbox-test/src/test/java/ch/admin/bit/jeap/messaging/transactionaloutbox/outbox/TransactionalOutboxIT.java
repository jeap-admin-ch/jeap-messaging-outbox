package ch.admin.bit.jeap.messaging.transactionaloutbox.outbox;

import ch.admin.bit.jeap.messaging.kafka.KafkaConfiguration;
import ch.admin.bit.jeap.messaging.kafka.contract.ContractsValidator;
import ch.admin.bit.jeap.messaging.kafka.interceptor.JeapKafkaMessageCallback;
import ch.admin.bit.jeap.messaging.kafka.properties.KafkaProperties;
import ch.admin.bit.jeap.messaging.kafka.test.KafkaIntegrationTestBase;
import ch.admin.bit.jeap.messaging.kafka.tracing.TraceContext;
import ch.admin.bit.jeap.messaging.kafka.tracing.TraceContextProvider;
import ch.admin.bit.jeap.messaging.transactionaloutbox.test.TestEvent;
import ch.admin.bit.jeap.messaging.transactionaloutbox.test.TestMessageKey;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.micrometer.tracing.test.autoconfigure.AutoConfigureTracing;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.test.annotation.Commit;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.transaction.TestTransaction;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@SuppressWarnings("SameParameterValue")
@DirtiesContext
@AutoConfigureTracing
@ExtendWith(MockitoExtension.class)
@SpringBootTest(properties = {
        "jeap.messaging.kafka.exposeMessageKeyToConsumer=true",
        "management.tracing.sampling.probability=1.0"
})
@Slf4j
class TransactionalOutboxIT extends KafkaIntegrationTestBase {

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Autowired
    DeferredMessageRepository deferredMessageRepository;
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Autowired
    private TransactionalOutbox transactionalOutbox;
    @SuppressWarnings("unused")
    @MockitoBean
    private ContractsValidator contractsValidator; // Disable contract checking by mocking the contracts validator
    @MockitoBean
    private TestEventListener testEventListener;
    @MockitoBean
    private JeapKafkaMessageCallback jeapKafkaMessageCallback;
    @Captor
    private ArgumentCaptor<TestEvent> testEventArgumentCaptor;
    @Captor
    private ArgumentCaptor<TestMessageKey> testMessageKeyArgumentCaptor;
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Autowired
    private KafkaConfiguration kafkaConfiguration;

    @Autowired
    private Tracer tracer;
    @Autowired
    private TraceContextProvider traceContextProvider;
    @Autowired
    DeferredMessageTestUtil deferredMessageTestUtil;


    @AfterEach
    void cleanUp() {
        deferredMessageTestUtil.deleteAllMessages();
    }

    @Transactional
    @Commit
    @Test
    void testOutboxSend() {

        // Start a span explicitly because Spring's test-transaction manager is a plain JpaTransactionManager that
        // does not get wrapped in a tracing manager. We want to verify that the jEAP outbox captures the currently
        // active trace context when sendMessage is called, so we open a scope here.
        Span junitSpan = tracer.nextSpan().name("junit").start();
        try (Tracer.SpanInScope _ = tracer.withSpan(junitSpan)) {
            TraceContext expectedTraceContext = traceContextProvider.getTraceContext();
            assertThat(expectedTraceContext).isNotNull();

            assertThat(deferredMessageRepository.findAll()).isEmpty();

            final String idempotenceIdEvent1 = "idempotenceId1";
            final String idempotenceIdEvent2 = "idempotenceId2";
            final TestEvent testEvent1 = TestEventBuilder.create().idempotenceId(idempotenceIdEvent1).build();
            final TestEvent testEvent2 = TestEventBuilder.create().idempotenceId(idempotenceIdEvent2).build();
            final TestMessageKey testMessageKey = TestMessageKey.newBuilder().setSomeProperty("someValue").build();

            transactionalOutbox.sendMessage(testEvent1, testMessageKey, TestEventConsumer.TOPIC);
            transactionalOutbox.sendMessageScheduled(testEvent2, TestEventConsumer.TOPIC);

            TestTransaction.end();
            verify(testEventListener, timeout(TEST_TIMEOUT).times(2)).receive(testEventArgumentCaptor.capture(), testMessageKeyArgumentCaptor.capture());
            final List<TestEvent> receivedEvents = testEventArgumentCaptor.getAllValues();
            assertThat(receivedEvents.stream().map(event -> event.getIdentity().getIdempotenceId()).toList())
                    .containsOnly(idempotenceIdEvent1, idempotenceIdEvent2);
            final List<TestMessageKey> receivedKeys = testMessageKeyArgumentCaptor.getAllValues();
            assertThat(receivedKeys).containsOnly(testMessageKey, null);

            // Make sure the relay process finished sending the scheduled message
            deferredMessageTestUtil.waitTillAllMessagesProcessed();

            TestTransaction.start();
            final List<DeferredMessage> deferredMessages = deferredMessageRepository.findAll();
            assertThat(deferredMessages).hasSize(2);
            assertThat(deferredMessages.stream().map(DeferredMessage::getMessageIdempotenceId).toList())
                    .containsOnly(idempotenceIdEvent1, idempotenceIdEvent2);
            assertThat(deferredMessages.get(0).getSentImmediately()).isNotNull();
            assertThat(deferredMessages.get(0).getFailed()).isNull();
            assertThat(deferredMessages.get(1).getSentScheduled()).isNotNull();
            assertThat(deferredMessages.get(1).getFailed()).isNull();
            Slice<Long> sentImmediatelyBeforeOrSentScheduledBefore = deferredMessageRepository.findSentImmediatelyBeforeOrSentScheduledBefore(ZonedDateTime.now(), Pageable.ofSize(10));
            deferredMessageRepository.deleteAllById(sentImmediatelyBeforeOrSentScheduledBefore.toSet());
            TestTransaction.end();

            final OutboxTraceContext actualTraceContext = deferredMessages.getFirst().getTraceContext();
            assertThat(actualTraceContext.getTraceIdHigh()).isEqualTo(expectedTraceContext.getTraceIdHigh());
            assertThat(actualTraceContext.getTraceId()).isEqualTo(expectedTraceContext.getTraceId());
            assertThat(actualTraceContext.getSpanId()).isEqualTo(expectedTraceContext.getSpanId());
            assertThat(actualTraceContext.getParentSpanId()).isEqualTo(expectedTraceContext.getParentSpanId());
            assertThat(actualTraceContext.getTraceIdString()).isEqualTo(expectedTraceContext.getTraceIdString());
            assertThat(actualTraceContext.getSampled()).isEqualTo(expectedTraceContext.getSampled());
            assertThat(deferredMessages.stream().filter(dm -> dm.getTraceContext().getTraceId().equals(actualTraceContext.getTraceId())).count()).isEqualTo(2);
            assertHeaderFromConsumedMessage(actualTraceContext.getTraceIdString(), 2);

            verify(jeapKafkaMessageCallback).onSend(testEvent1, TestEventConsumer.TOPIC);
            verify(jeapKafkaMessageCallback).onSend(testEvent2, TestEventConsumer.TOPIC);
        } finally {
            junitSpan.end();
        }
    }

    @Transactional
    @Commit
    @Test
    void testMessageRelay_assertMessageSavedForUnknownClusterIsProducedToTheDefaultCluster() {
        // send a message with the outbox (scheduled)
        assertThat(deferredMessageRepository.findAll()).isEmpty();
        final String idempotenceIdEvent = UUID.randomUUID().toString();
        final TestEvent testEvent = TestEventBuilder.create().idempotenceId(idempotenceIdEvent).build();
        transactionalOutbox.sendMessageScheduled(testEvent, TestEventConsumer.TOPIC);
        TestTransaction.end();

        // Make sure the relay process finished sending the scheduled message
        deferredMessageTestUtil.waitTillAllMessagesProcessed();

        // Alter the message in the database to set the cluster name to 'unknown' and to clear the sent flag in order
        // to make the relay process send the message again, this time for the cluster named 'unknown'.
        TestTransaction.start();
        deferredMessageTestUtil.alterClusterNameAndMakeAvailableForMessageRelay(idempotenceIdEvent, "unknown");
        TestTransaction.end();

        // Make sure the relay process finished sending the altered scheduled message
        deferredMessageTestUtil.waitTillAllMessagesProcessed();

        // Verify that two events have been sent: the one for the default cluster (named 'default') and the one
        // for the non-existing 'unknown' cluster.
        verify(testEventListener, timeout(TEST_TIMEOUT).times(2)).receive(testEventArgumentCaptor.capture(), testMessageKeyArgumentCaptor.capture());
    }


    private void assertHeaderFromConsumedMessage(String traceIdString, int count) {
        final Consumer<Object, Object> consumer = createConsumer(kafkaConfiguration);
        consumer.assign(Collections.singletonList(new TopicPartition(TestEventConsumer.TOPIC, 0)));
        consumer.seekToBeginning(Collections.singletonList(new TopicPartition(TestEventConsumer.TOPIC, 0)));
        final ConsumerRecords<Object, Object> records = consumer.poll(Duration.ofSeconds(10));
        consumer.close();

        for (ConsumerRecord<Object, Object> currentRecord : records) {
            final String headerTraceId = (new String(currentRecord.headers().lastHeader("traceparent").value())).split("-")[1];
            assertThat(headerTraceId).isEqualTo(traceIdString);
        }
        assertThat(records.count()).isEqualTo(count);
    }

    private static Consumer<Object, Object> createConsumer(KafkaConfiguration kafkaConfiguration) {
        Map<String, Object> props = new HashMap<>(kafkaConfiguration.consumerConfig(KafkaProperties.DEFAULT_CLUSTER));
        // We are resending messages exactly as received i.e. as byte array of the original message
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        // Remove the interceptor configured by the domain event library, because it expects messages to be domain events.
        props.remove(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG);
        return new KafkaConsumer<>(props);
    }
}
