package ch.admin.bit.jeap.messaging.transactionaloutbox.outbox;

import ch.admin.bit.jeap.messaging.kafka.test.KafkaIntegrationTestBase;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Commit;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("SameParameterValue")
@DirtiesContext
@AutoConfigureObservability
@SpringBootTest(properties = {
        "jeap.messaging.transactional-outbox.house-keeping-max-pages=3",
        "jeap.messaging.transactional-outbox.house-keeping-page-size=2"}
)
@Slf4j
class OutboxHouseKeepingIT extends KafkaIntegrationTestBase {

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Autowired
    DeferredMessageRepository deferredMessageRepository;

    @Autowired
    DeferredMessageTestUtil deferredMessageTestUtil;

    @Autowired
    OutboxHouseKeeping outboxHouseKeeping;

    @AfterEach
    void cleanUp() {
        deferredMessageTestUtil.deleteAllMessages();
    }

    @Transactional
    @Commit
    @Test
    void testHousekeeping() {

        assertThat(deferredMessageRepository.findAll()).isEmpty();

        int countSentImmediatelyBeforeMessages = 5;
        while (countSentImmediatelyBeforeMessages-- > 0) {
            DeferredMessage messageSentImmediatelyBefore = createMessage();
            ReflectionTestUtils.setField(messageSentImmediatelyBefore, "sentImmediately", ZonedDateTime.now().minusDays(3));
            deferredMessageRepository.save(messageSentImmediatelyBefore);
        }

        int countSentScheduledBeforeMessages = 5;
        while (countSentScheduledBeforeMessages-- > 0) {
            DeferredMessage messageSentScheduledBefore = createMessage();
            ReflectionTestUtils.setField(messageSentScheduledBefore, "sentScheduled", ZonedDateTime.now().minusDays(3));
            deferredMessageRepository.save(messageSentScheduledBefore);
        }

        int countUnsentMessagesCreatedBefore = 5;
        while (countUnsentMessagesCreatedBefore-- > 0) {
            DeferredMessage messageCreated = createMessage();
            ReflectionTestUtils.setField(messageCreated, "sentImmediately", null);
            ReflectionTestUtils.setField(messageCreated, "sentScheduled", null);
            ReflectionTestUtils.setField(messageCreated, "created", ZonedDateTime.now().minusDays(32));
            deferredMessageRepository.save(messageCreated);
        }

        assertThat(deferredMessageRepository.findAll()).hasSize(15);

        outboxHouseKeeping.deleteOldMessages();

        assertThat(deferredMessageRepository.findAll()).hasSize(4);

        outboxHouseKeeping.deleteOldMessages();

        assertThat(deferredMessageRepository.findAll()).isEmpty();

    }

    private DeferredMessage createMessage() {
        return DeferredMessage.builder()
                .message("foo".getBytes(StandardCharsets.UTF_8))
                .messageId(UUID.randomUUID().toString())
                .topic("topic")
                .messageIdempotenceId(UUID.randomUUID().toString())
                .messageTypeName("foo")
                .build();
    }


}
