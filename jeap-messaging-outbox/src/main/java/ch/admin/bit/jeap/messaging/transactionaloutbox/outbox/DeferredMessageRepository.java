package ch.admin.bit.jeap.messaging.transactionaloutbox.outbox;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Set;

public interface DeferredMessageRepository {

    DeferredMessage getById(long id);

    DeferredMessage save(DeferredMessage deferredMessage);

    void deleteById(long id);

    void markSentImmediately(long id, ZonedDateTime sentTime);

    void markSentScheduled(long id, ZonedDateTime sentTime);

    void markFailed(long id, ZonedDateTime failedTime, SendFailureReason failReason);

    void markForResend(long id, boolean resend);

    void setScheduleAfter(long id, ZonedDateTime scheduleAfter);

    List<DeferredMessage> findMessagesReadyToBeSent(int numMessages);

    Slice<Long> findSentImmediatelyBeforeOrSentScheduledBefore(ZonedDateTime timestamp, Pageable pageable);

    Slice<Long> findSentImmediatelyIsNullAndSentScheduledIsNullAndCreatedBefore(ZonedDateTime timestamp, Pageable pageable);

    void deleteAllById(Set<Long> ids);

    int countMessagesReadyToBeSent();

    List<DeferredMessage> findAll();

}
