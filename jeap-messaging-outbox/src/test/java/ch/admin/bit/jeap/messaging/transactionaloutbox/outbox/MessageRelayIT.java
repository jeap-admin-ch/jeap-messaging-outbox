package ch.admin.bit.jeap.messaging.transactionaloutbox.outbox;

import ch.admin.bit.jeap.messaging.kafka.contract.ContractsValidator;
import ch.admin.bit.jeap.messaging.kafka.metrics.KafkaMessagingMetrics;
import ch.admin.bit.jeap.messaging.kafka.signature.publisher.SignaturePublisherProperties;
import ch.admin.bit.jeap.messaging.transactionaloutbox.outbox.testsupport.DeferredMessageTestUtil;
import ch.admin.bit.jeap.messaging.transactionaloutbox.outbox.testsupport.OutboxMockKafkaTestConfig;
import ch.admin.bit.jeap.messaging.transactionaloutbox.outbox.testsupport.StringMessage;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.test.annotation.Commit;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.transaction.TestTransaction;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@DataJpaTest(properties = {
        "jeap.messaging.transactional-outbox.poll-delay=1s",
        "jeap.messaging.transactional-outbox.message-relay-batch-size=2",
        "jeap.messaging.transactional-outbox.continuous-relay-timeout=3s"})
@ContextConfiguration(classes = OutboxMockKafkaTestConfig.class)
class MessageRelayIT {

    @Autowired
    TransactionalOutbox transactionalOutbox;

    @Autowired
    DeferredMessageRepository deferredMessageRepository;

    @Autowired
    MessageSerializer messageSerializer;

    @MockitoBean
    DeferredMessageSender deferredMessageSenderMock;

    @MockitoBean
    ContractsValidator contractsValidator;

    @MockitoBean
    OutboxTracing outboxTracing;

    @MockitoBean
    KafkaMessagingMetrics kafkaMessagingMetrics;

    @MockitoBean
    @SuppressWarnings("unused")
    SignaturePublisherProperties signaturePublisherProperties;

    @Commit
    @Transactional
    @Test
    void testRelay() {
        DeferredMessageTestUtil.with(deferredMessageRepository).deleteAllMessagesAfter(() -> {
            when(outboxTracing.retrieveCurrentTraceContext()).thenReturn(OutboxTraceContext.builder().traceId(1L).spanId(2L).parentSpanId(3L).sampled(Boolean.TRUE).build());
            assertThat(deferredMessageRepository.findAll()).isEmpty();
            IntStream.range(1, 9).mapToObj(i -> StringMessage.from("test-message-" + i))
                    .forEach(message -> transactionalOutbox.sendMessageScheduled(message, "test-topic"));
            final ZonedDateTime afterSend = ZonedDateTime.now();
            TestTransaction.end();
            doAnswer(_ -> {
                Thread.sleep(500);
                return null;
            }).when(deferredMessageSenderMock).sendAsScheduled(any(DeferredMessage.class));

            await().atMost(Duration.ofSeconds(10)).until(() ->
                    deferredMessageRepository.findMessagesReadyToBeSent(1).isEmpty()
            );

            TestTransaction.start();
            List<DeferredMessage> allDeferredMessages = deferredMessageRepository.findAll();
            allDeferredMessages.forEach(deferredMessage -> {
                assertThat(deferredMessage.getSentScheduled()).isNotNull();
                assertThat(deferredMessage.getSentScheduled()).isAfterOrEqualTo(afterSend);
                assertThat(deferredMessage.getSentScheduled()).isBeforeOrEqualTo(ZonedDateTime.now());
                assertThat(deferredMessage.getTraceContext().getTraceId()).isEqualTo(1L);
                assertThat(deferredMessage.getTraceContext().getSpanId()).isEqualTo(2L);
                assertThat(deferredMessage.getTraceContext().getParentSpanId()).isEqualTo(3L);
                assertThat(deferredMessage.getTraceContext().getSampled()).isTrue();
            });
            TestTransaction.end();
        });
    }

    @Commit
    @Transactional
    @Test
    void testRelay_withoutClusterName() {
        DeferredMessageTestUtil.with(deferredMessageRepository).deleteAllMessagesAfter(() -> {
            assertThat(deferredMessageRepository.findAll()).isEmpty();

            StringMessage message = StringMessage.from("test");
            DeferredMessage deferredMessage = DeferredMessage.builder()
                    .message(new byte[]{1, 2, 3})
                    .key(null)
                    .clusterName(null)
                    .topic("the-topic")
                    .messageId(message.getMessageId())
                    .messageIdempotenceId(message.getIdentity().getIdempotenceId())
                    .messageTypeName(message.getType().getName())
                    .messageTypeVersion(message.getType().getVersion())
                    .sendImmediately(false)
                    .traceContext(null)
                    .build();
            deferredMessageRepository.save(deferredMessage);
            TestTransaction.end();

            await().atMost(Duration.ofSeconds(10)).until(() ->
                    deferredMessageRepository.findMessagesReadyToBeSent(1).isEmpty()
            );

            TestTransaction.start();
            List<DeferredMessage> allDeferredMessages = deferredMessageRepository.findAll();
            assertThat(allDeferredMessages)
                    .hasSize(1);
            DeferredMessage sentMessage = allDeferredMessages.getFirst();
            assertThat(sentMessage.getMessageIdempotenceId()).isEqualTo(deferredMessage.getMessageIdempotenceId());
            assertThat(sentMessage.getSentScheduled()).isNotNull();
            assertThat(sentMessage.getFailed()).isNull();
            assertThat(sentMessage.getFailReason()).isNull();
            TestTransaction.end();
        });
    }

    @Commit
    @Transactional
    @Test
    void testRelayWithFailedMessage() {
        DeferredMessageTestUtil.with(deferredMessageRepository).deleteAllMessagesAfter(() -> {
            when(outboxTracing.retrieveCurrentTraceContext()).thenReturn(OutboxTraceContext.builder().traceId(1L).spanId(2L).parentSpanId(3L).build());
            assertThat(deferredMessageRepository.findAll()).isEmpty();
            final String unauthorizedTopic = "unauthorized-topic";
            final String authorizedTopic = "authorized-topic";
            final StringMessage unauthorizedTestMessage = StringMessage.from("unauthorized-test-message");
            final StringMessage authorizedTestMessage1 = StringMessage.from("authorized-test-message-1");
            final StringMessage authorizedTestMessage2 = StringMessage.from("authorized-test-message-2");
            Mockito.doAnswer(invocation -> {
                DeferredMessage deferredMessage = invocation.getArgument(0, DeferredMessage.class);
                // Make messages to the unauthorized topic fail with a topic authorization exception
                if (unauthorizedTopic.equals(deferredMessage.getTopic())) {
                    throw DeferredMessageSendException.topicAuthorizationException(deferredMessage, new RuntimeException("unauthorized on topic"));
                } else {
                    // Let all other messages pass successfully
                    return null;
                }
            }).when(deferredMessageSenderMock).sendAsScheduled(any(DeferredMessage.class));

            transactionalOutbox.sendMessageScheduled(unauthorizedTestMessage, unauthorizedTopic);
            transactionalOutbox.sendMessageScheduled(authorizedTestMessage1, authorizedTopic);
            transactionalOutbox.sendMessageScheduled(authorizedTestMessage2, authorizedTopic);

            final ZonedDateTime afterSend = ZonedDateTime.now();
            TestTransaction.end();
            await().atMost(Duration.ofSeconds(5)).until(() ->
                    deferredMessageRepository.findMessagesReadyToBeSent(1).isEmpty()
            );
            TestTransaction.start();
            List<DeferredMessage> allDeferredMessages = deferredMessageRepository.findAll();
            allDeferredMessages.sort(Comparator.comparing(DeferredMessage::getId));
            assertThat(allDeferredMessages.getFirst().getSentScheduled()).isNull();
            assertThat(allDeferredMessages.getFirst().getFailed()).isNotNull();
            assertThat(allDeferredMessages.getFirst().getFailed()).isAfterOrEqualTo(afterSend);
            assertThat(allDeferredMessages.get(1).getFailed()).isNull();
            assertThat(allDeferredMessages.get(1).getSentScheduled()).isNotNull();
            assertThat(allDeferredMessages.get(1).getSentScheduled()).isAfterOrEqualTo(afterSend);
            assertThat(allDeferredMessages.get(2).getFailed()).isNull();
            assertThat(allDeferredMessages.get(2).getSentScheduled()).isNotNull();
            assertThat(allDeferredMessages.get(2).getSentScheduled()).isAfterOrEqualTo(afterSend);
            TestTransaction.end();
        });
    }

    @Commit
    @Transactional
    @Test
    void testSendFailResendMessages() {
        DeferredMessageTestUtil.with(deferredMessageRepository).deleteAllMessagesAfter(() -> {
            when(outboxTracing.retrieveCurrentTraceContext()).thenReturn(OutboxTraceContext.builder().traceId(1L).spanId(2L).parentSpanId(3L).build());
            assertThat(deferredMessageRepository.findAll()).isEmpty();
            final StringMessage testMessage1 = StringMessage.from("test-message-1");
            final StringMessage testMessage2 = StringMessage.from("test-message-2");
            final StringMessage testMessage3 = StringMessage.from("test-message-3");
            final ArgumentCaptor<DeferredMessage> deferredMessageCaptor = ArgumentCaptor.forClass(DeferredMessage.class);
            Mockito.doAnswer(invocation -> {
                DeferredMessage deferredMessage = invocation.getArgument(0, DeferredMessage.class);
                // Make testMessage1 and testMessage2 fail because of a TopicAuthorizationException
                if (Arrays.equals(deferredMessage.getMessage(), messageSerializer.serializeMessage(testMessage1, "test-topic-1")) ||
                        Arrays.equals(deferredMessage.getMessage(), messageSerializer.serializeMessage(testMessage2, "test-topic-2"))) {
                    throw DeferredMessageSendException.topicAuthorizationException(deferredMessage, new RuntimeException("unauthorized on topic"));
                } else {
                    // Let all other messages pass successfully
                    return null;
                }
            }).when(deferredMessageSenderMock).sendAsScheduled(any(DeferredMessage.class));
            final ZonedDateTime beforeSend = ZonedDateTime.now();
            transactionalOutbox.sendMessageScheduled(testMessage1, "test-key-1", "test-topic-1");
            transactionalOutbox.sendMessageScheduled(testMessage2, "test-key-2", "test-topic-2");
            transactionalOutbox.sendMessageScheduled(testMessage3, "test-key-3", "test-topic-3");

            // make messages visible to relay process and wait for them to be sent
            TestTransaction.end();
            verify(deferredMessageSenderMock, timeout(5000).times(3)).sendAsScheduled(deferredMessageCaptor.capture());
            verifyNoMoreInteractions(deferredMessageSenderMock);

            // wait for relay send transaction to be committed
            DeferredMessageTestUtil.with(deferredMessageRepository).waitForMessageSentOrFailed(deferredMessageCaptor.getValue().getId());

            TestTransaction.start();
            final ZonedDateTime afterDelivery = ZonedDateTime.now();
            assertFailedMessages(beforeSend, afterDelivery, 2, 0);
            FailedMessage failedMessage = transactionalOutbox.findFailedMessages(0, afterDelivery, false, 1).getFirst();


            // Reset mock to no longer fail previously failed messages
            Mockito.reset(deferredMessageSenderMock);

            transactionalOutbox.resendMessageScheduled(failedMessage.getId());

            assertFailedMessages(beforeSend, afterDelivery, 1, 1);

            // make message marked resend visible to relay process and wait for it to be sent
            TestTransaction.end();
            verify(deferredMessageSenderMock, timeout(5000).times(1)).sendAsScheduled(deferredMessageCaptor.capture());
            verifyNoMoreInteractions(deferredMessageSenderMock);

            // Wait for relay process send transaction to be committed
            DeferredMessageTestUtil.with(deferredMessageRepository).waitForMessageSentOrFailed(deferredMessageCaptor.getValue().getId());

            TestTransaction.start();
            assertFailedMessages(beforeSend, afterDelivery, 1, 0);
            TestTransaction.end();
        });
    }

    private void assertFailedMessages(ZonedDateTime startingFrom, ZonedDateTime before, int expectedNumFailedNotInResend, int expectedNumFailedInResend) {
        assertThat(transactionalOutbox.countFailedMessages(false)).isEqualTo(expectedNumFailedNotInResend);
        assertThat(transactionalOutbox.countFailedMessages(true)).isEqualTo(expectedNumFailedInResend);
        assertThat(transactionalOutbox.countFailedMessages(startingFrom, before, false)).isEqualTo(expectedNumFailedNotInResend);
        assertThat(transactionalOutbox.countFailedMessages(startingFrom, before, true)).isEqualTo(expectedNumFailedInResend);
        List<FailedMessage> failedMessages = transactionalOutbox.findFailedMessages(startingFrom, before, false, expectedNumFailedNotInResend + 1);
        assertThat(failedMessages).hasSize(expectedNumFailedNotInResend);
        List<FailedMessage> failedMessagesInResend = transactionalOutbox.findFailedMessages(startingFrom, before, true, expectedNumFailedInResend + 1);
        assertThat(failedMessagesInResend).hasSize(expectedNumFailedInResend);
        if (failedMessages.size() > 1) {
            List<FailedMessage> failedMessagesSublist = transactionalOutbox.findFailedMessages(failedMessages.getFirst().getId(), before, false, failedMessages.size());
            assertThat(failedMessagesSublist).hasSize(failedMessages.size() - 1);
        }
        if (failedMessagesInResend.size() > 1) {
            List<FailedMessage> failedMessagesSublistInResend = transactionalOutbox.findFailedMessages(failedMessages.getFirst().getId(), before, true, failedMessagesInResend.size());
            assertThat(failedMessagesSublistInResend).hasSize(failedMessagesSublistInResend.size() - 1);
        }
    }

}
