package ch.admin.bit.jeap.messaging.transactionaloutbox.outbox;

import ch.admin.bit.jeap.messaging.kafka.KafkaConfiguration;
import ch.admin.bit.jeap.messaging.kafka.contract.ContractsValidator;
import ch.admin.bit.jeap.messaging.kafka.properties.KafkaProperties;
import ch.admin.bit.jeap.messaging.kafka.test.KafkaIntegrationTestBase;
import ch.admin.bit.jeap.messaging.transactionaloutbox.headers.OutboxMessageHeadersRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.micrometer.tracing.test.autoconfigure.AutoConfigureTracing;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.util.AopTestUtils;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;

@SpringBootTest(properties = {
        "spring.application.name=jme-messaging-receiverpublisher-outbox-service",
        "management.tracing.sampling.probability=1.0",
        "jeap.messaging.transactional-outbox.headers-enabled=true",
        "jeap.messaging.transactional-outbox.scheduled-relay-enabled=false",
        "spring.flyway.locations=classpath:db/migration/common,classpath:db/migration/headers",
        "spring.jpa.hibernate.ddl-auto=validate"})
@DirtiesContext
@ActiveProfiles("test-signing")
@AutoConfigureTracing
class TransactionalOutboxHeadersIT extends KafkaIntegrationTestBase {

    @Autowired
    private TransactionalOutbox outbox;
    @Autowired
    private PlatformTransactionManager transactionManager;
    @Autowired
    private MessageRelay relay;
    @Autowired
    private DeferredMessageRepository messages;
    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private KafkaConfiguration kafkaConfiguration;
    @MockitoBean
    private ContractsValidator contractsValidator;
    @MockitoSpyBean
    private OutboxMessageHeadersRepository headerRepository;

    @AfterEach
    void cleanUp() {
        new TransactionTemplate(transactionManager).executeWithoutResult(_ ->
                messages.findAll().forEach(message -> messages.deleteById(message.getId())));
        assertThat(headerCount()).isZero();
    }

    @Test
    void immediateSendPreservesOrderedDuplicateBinaryAndNullHeaders() {
        try (KafkaConsumer<byte[], byte[]> consumer = consumerAtEnd()) {
            byte[] value = {0, 1, (byte) 255};
            RecordHeaders headers = new RecordHeaders();
            headers.add("test-binary", value).add("test-binary", new byte[0]).add("test-null", null);
            new TransactionTemplate(transactionManager).executeWithoutResult(_ -> {
                outbox.sendMessage(event(), null, TestEventConsumer.TOPIC, headers);
                // Enqueue takes a snapshot. Caller mutations before commit must not change delivery.
                value[0] = 42;
                headers.remove("test-null");
            });

            ConsumerRecord<byte[], byte[]> record = receiveOne(consumer);
            assertThat(record.headers().lastHeader("jeap-sign").value()).isNotEmpty();
            assertThat(record.headers().lastHeader("jeap-cert").value()).isNotEmpty();
            assertThat(record.headers().lastHeader("traceparent")).isNotNull();
            var binaryHeaders = record.headers().headers("test-binary").iterator();
            assertThat(binaryHeaders.next().value()).containsExactly(0, 1, (byte) 255);
            assertThat(binaryHeaders.next().value()).isEmpty();
            assertThat(binaryHeaders.hasNext()).isFalse();
            assertThat(record.headers().lastHeader("test-null")).isNotNull();
            assertThat(record.headers().lastHeader("test-null").value()).isNull();
            assertThat(headerCount()).isEqualTo(3);
        }
    }

    @Test
    void scheduledDeliveryAndResendReloadHeadersFromDatabase() {
        try (KafkaConsumer<byte[], byte[]> consumer = consumerAtEnd()) {
            new TransactionTemplate(transactionManager).executeWithoutResult(_ ->
                    outbox.sendMessageScheduled(event(), null, TestEventConsumer.TOPIC,
                            new RecordHeaders().add("test-target", new byte[]{7})));
            long messageId = messages.findAll().getFirst().getId();

            // Relay in a new transaction loads a new entity; no headers are kept on DeferredMessage.
            new TransactionTemplate(transactionManager).executeWithoutResult(_ -> relay.relay());
            assertThat(receiveOne(consumer).headers().lastHeader("test-target").value()).containsExactly(7);
            jdbc.update("UPDATE deferred_message SET sent_scheduled = NULL WHERE id = ?", messageId);
            new TransactionTemplate(transactionManager).executeWithoutResult(_ -> relay.relay());
            assertThat(receiveOne(consumer).headers().lastHeader("test-target").value()).containsExactly(7);
            assertThat(headerCount()).isEqualTo(1);
        }
    }

    @Test
    void rollbackRemovesBothMessageAndHeadersAndDoesNotPublish() {
        try (KafkaConsumer<byte[], byte[]> consumer = consumerAtEnd()) {
            new TransactionTemplate(transactionManager).executeWithoutResult(status -> {
                outbox.sendMessage(event(), null, TestEventConsumer.TOPIC,
                        new RecordHeaders().add("test-target", new byte[]{7}));
                assertThat(headerCount()).isEqualTo(1);
                status.setRollbackOnly();
            });
            assertThat(messages.findAll()).isEmpty();
            assertThat(headerCount()).isZero();
            assertThat(consumer.poll(Duration.ofSeconds(1)).isEmpty()).isTrue();
        }
    }

    @Test
    void headerStorageFailureRollsBackEnqueue() {
        OutboxMessageHeadersRepository target = AopTestUtils.getUltimateTargetObject(headerRepository);
        doThrow(new IllegalStateException("header storage unavailable"))
                .when(target).saveHeaders(anyLong(), anyList());
        assertThatThrownBy(() -> new TransactionTemplate(transactionManager).executeWithoutResult(_ ->
                outbox.sendMessage(event(), null, TestEventConsumer.TOPIC,
                        new RecordHeaders().add("test-target", new byte[]{7}))))
                .isInstanceOf(IllegalStateException.class);
        assertThat(messages.findAll()).isEmpty();
        assertThat(headerCount()).isZero();
    }

    @Test
    void existingHeaderlessMessageCanBeRelayedWithHeadersEnabled() {
        try (KafkaConsumer<byte[], byte[]> consumer = consumerAtEnd()) {
            new TransactionTemplate(transactionManager).executeWithoutResult(_ ->
                    outbox.sendMessageScheduled(event(), TestEventConsumer.TOPIC));
            new TransactionTemplate(transactionManager).executeWithoutResult(_ -> relay.relay());
            assertThat(receiveOne(consumer).headers().lastHeader("test-target")).isNull();
            assertThat(headerCount()).isZero();
        }
    }

    @Test
    void headerReadFailureDoesNotSendAnUnsignedOrUntargetedFallback() {
        try (KafkaConsumer<byte[], byte[]> consumer = consumerAtEnd()) {
            new TransactionTemplate(transactionManager).executeWithoutResult(_ ->
                    outbox.sendMessageScheduled(event(), null, TestEventConsumer.TOPIC,
                            new RecordHeaders().add("test-target", new byte[]{7})));
            OutboxMessageHeadersRepository target = AopTestUtils.getUltimateTargetObject(headerRepository);
            doThrow(new IllegalStateException("header storage unavailable")).when(target).findHeaders(anyLong());

            new TransactionTemplate(transactionManager).executeWithoutResult(_ -> relay.relay());

            assertThat(consumer.poll(Duration.ofSeconds(1)).isEmpty()).isTrue();
            assertThat(messages.findAll()).singleElement().satisfies(message -> {
                assertThat(message.getSentScheduled()).isNull();
                assertThat(message.getSentImmediately()).isNull();
            });
            assertThat(headerCount()).isEqualTo(1);
        }
    }

    private int headerCount() {
        return jdbc.queryForObject("SELECT count(*) FROM deferred_message_header", Integer.class);
    }

    private ch.admin.bit.jeap.messaging.transactionaloutbox.test.TestEvent event() {
        return TestEventBuilder.create().idempotenceId(UUID.randomUUID().toString()).build();
    }

    private KafkaConsumer<byte[], byte[]> consumerAtEnd() {
        var props = new HashMap<>(kafkaConfiguration.consumerConfig(KafkaProperties.DEFAULT_CLUSTER));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.remove(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG);
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        var partition = new TopicPartition(TestEventConsumer.TOPIC, 0);
        consumer.assign(List.of(partition));
        consumer.seekToEnd(List.of(partition));
        consumer.position(partition);
        return consumer;
    }

    private ConsumerRecord<byte[], byte[]> receiveOne(KafkaConsumer<byte[], byte[]> consumer) {
        var records = new ArrayList<ConsumerRecord<byte[], byte[]>>();
        await().atMost(Duration.ofSeconds(10)).until(() -> {
            consumer.poll(Duration.ofMillis(100)).forEach(records::add);
            return !records.isEmpty();
        });
        assertThat(records).hasSize(1);
        return records.getFirst();
    }
}
