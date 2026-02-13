package ch.admin.bit.jeap.messaging.transactionaloutbox.outbox;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OutboxHouseKeepingTest {

    @Mock
    private DeferredMessageRepository deferredMessageRepository;

    @Mock
    private TransactionalOutboxConfiguration config;

    @Mock
    private TransactionTemplate transactionTemplate;

    @Captor
    private ArgumentCaptor<Set<Long>> messageIdsCaptor;

    @Captor
    private ArgumentCaptor<ZonedDateTime> dateTimeCaptor;

    private OutboxHouseKeeping outboxHouseKeeping;

    @BeforeEach
    void setup() {
        when(config.getHouseKeepingPageSize()).thenReturn(10);
        when(config.getHouseKeepingMaxPages()).thenReturn(100);
        when(config.getSentMessageRetentionDuration()).thenReturn(Duration.ofDays(7));
        when(config.getUnsentMessageRetentionDuration()).thenReturn(Duration.ofDays(30));

        when(transactionTemplate.execute(any())).thenAnswer(invocation -> {
            TransactionCallback<?> callback = invocation.getArgument(0);
            return callback.doInTransaction(null);
        });

        outboxHouseKeeping = new OutboxHouseKeeping(deferredMessageRepository, config, transactionTemplate);
    }

    @Test
    void deleteOldMessages_WhenNoOldMessages_ThenNoMessagesDeleted() {
        Slice<Long> emptySlice = new SliceImpl<>(List.of());
        when(deferredMessageRepository.findSentImmediatelyBeforeOrSentScheduledBefore(any(), any())).thenReturn(emptySlice);
        when(deferredMessageRepository.findSentImmediatelyIsNullAndSentScheduledIsNullAndCreatedBefore(any(), any())).thenReturn(emptySlice);

        outboxHouseKeeping.deleteOldMessages();

        verify(deferredMessageRepository, atLeastOnce()).findSentImmediatelyBeforeOrSentScheduledBefore(any(), any());
        verify(deferredMessageRepository, atLeastOnce()).findSentImmediatelyIsNullAndSentScheduledIsNullAndCreatedBefore(any(), any());
        verify(deferredMessageRepository, never()).deleteAllById(any());
    }

    @Test
    void deleteOldMessages_WhenOldSentMessagesExist_ThenDeletesSentMessages() {
        Slice<Long> sentMessagesSlice = new SliceImpl<>(List.of(1L, 2L, 3L), Pageable.ofSize(10), false);
        Slice<Long> emptySlice = new SliceImpl<>(List.of());
        when(deferredMessageRepository.findSentImmediatelyBeforeOrSentScheduledBefore(any(), any())).thenReturn(sentMessagesSlice);
        when(deferredMessageRepository.findSentImmediatelyIsNullAndSentScheduledIsNullAndCreatedBefore(any(), any())).thenReturn(emptySlice);

        outboxHouseKeeping.deleteOldMessages();

        verify(deferredMessageRepository, times(1)).deleteAllById(messageIdsCaptor.capture());
        assertThat(messageIdsCaptor.getValue()).containsExactlyInAnyOrder(1L, 2L, 3L);
    }

    @Test
    void deleteOldMessages_WhenOldUnsentMessagesExist_ThenDeletesUnsentMessages() {
        Slice<Long> emptySlice = new SliceImpl<>(List.of());
        Slice<Long> unsentMessagesSlice = new SliceImpl<>(List.of(4L, 5L), Pageable.ofSize(10), false);
        when(deferredMessageRepository.findSentImmediatelyBeforeOrSentScheduledBefore(any(), any())).thenReturn(emptySlice);
        when(deferredMessageRepository.findSentImmediatelyIsNullAndSentScheduledIsNullAndCreatedBefore(any(), any())).thenReturn(unsentMessagesSlice);

        outboxHouseKeeping.deleteOldMessages();

        verify(deferredMessageRepository, times(1)).deleteAllById(messageIdsCaptor.capture());
        assertThat(messageIdsCaptor.getValue()).containsExactlyInAnyOrder(4L, 5L);
    }

    @Test
    void deleteOldMessages_WhenBothOldSentAndUnsentMessagesExist_ThenDeletesBothTypes() {
        Slice<Long> sentMessagesSlice = new SliceImpl<>(List.of(1L, 2L), Pageable.ofSize(10), false);
        Slice<Long> unsentMessagesSlice = new SliceImpl<>(List.of(3L, 4L), Pageable.ofSize(10), false);
        when(deferredMessageRepository.findSentImmediatelyBeforeOrSentScheduledBefore(any(), any())).thenReturn(sentMessagesSlice);
        when(deferredMessageRepository.findSentImmediatelyIsNullAndSentScheduledIsNullAndCreatedBefore(any(), any())).thenReturn(unsentMessagesSlice);

        outboxHouseKeeping.deleteOldMessages();

        verify(deferredMessageRepository, times(2)).deleteAllById(messageIdsCaptor.capture());
        assertThat(messageIdsCaptor.getAllValues()).hasSize(2);
        assertThat(messageIdsCaptor.getAllValues().get(0)).containsExactlyInAnyOrder(1L, 2L);
        assertThat(messageIdsCaptor.getAllValues().get(1)).containsExactlyInAnyOrder(3L, 4L);
    }

    @Test
    void deleteOldMessages_WhenMultiplePagesOfSentMessages_ThenDeletesAllPages() {
        Slice<Long> firstPage = new SliceImpl<>(List.of(1L, 2L, 3L), Pageable.ofSize(10), true);
        Slice<Long> secondPage = new SliceImpl<>(List.of(4L, 5L), Pageable.ofSize(10), false);
        Slice<Long> emptySlice = new SliceImpl<>(List.of());
        when(deferredMessageRepository.findSentImmediatelyBeforeOrSentScheduledBefore(any(), any()))
                .thenReturn(firstPage, secondPage);
        when(deferredMessageRepository.findSentImmediatelyIsNullAndSentScheduledIsNullAndCreatedBefore(any(), any()))
                .thenReturn(emptySlice);

        outboxHouseKeeping.deleteOldMessages();

        verify(deferredMessageRepository, times(2)).findSentImmediatelyBeforeOrSentScheduledBefore(any(), any());
        verify(deferredMessageRepository, times(2)).deleteAllById(messageIdsCaptor.capture());
        assertThat(messageIdsCaptor.getAllValues()).hasSize(2);
        assertThat(messageIdsCaptor.getAllValues().get(0)).containsExactlyInAnyOrder(1L, 2L, 3L);
        assertThat(messageIdsCaptor.getAllValues().get(1)).containsExactlyInAnyOrder(4L, 5L);
    }

    @Test
    void deleteOldMessages_WhenMultiplePagesOfUnsentMessages_ThenDeletesAllPages() {
        Slice<Long> emptySlice = new SliceImpl<>(List.of());
        Slice<Long> firstPage = new SliceImpl<>(List.of(1L, 2L, 3L), Pageable.ofSize(10), true);
        Slice<Long> secondPage = new SliceImpl<>(List.of(4L, 5L, 6L), Pageable.ofSize(10), false);
        when(deferredMessageRepository.findSentImmediatelyBeforeOrSentScheduledBefore(any(), any()))
                .thenReturn(emptySlice);
        when(deferredMessageRepository.findSentImmediatelyIsNullAndSentScheduledIsNullAndCreatedBefore(any(), any()))
                .thenReturn(firstPage, secondPage);

        outboxHouseKeeping.deleteOldMessages();

        verify(deferredMessageRepository, times(2)).findSentImmediatelyIsNullAndSentScheduledIsNullAndCreatedBefore(any(), any());
        verify(deferredMessageRepository, times(2)).deleteAllById(messageIdsCaptor.capture());
        assertThat(messageIdsCaptor.getAllValues()).hasSize(2);
        assertThat(messageIdsCaptor.getAllValues().get(0)).containsExactlyInAnyOrder(1L, 2L, 3L);
        assertThat(messageIdsCaptor.getAllValues().get(1)).containsExactlyInAnyOrder(4L, 5L, 6L);
    }

    @Test
    void deleteOldMessages_WhenMaxPagesReached_ThenStopsProcessing() {
        when(config.getHouseKeepingMaxPages()).thenReturn(2);
        outboxHouseKeeping = new OutboxHouseKeeping(deferredMessageRepository, config, transactionTemplate);

        Slice<Long> page = new SliceImpl<>(List.of(1L, 2L), Pageable.ofSize(10), true);
        when(deferredMessageRepository.findSentImmediatelyBeforeOrSentScheduledBefore(any(), any())).thenReturn(page);
        Slice<Long> emptySlice = new SliceImpl<>(List.of());
        when(deferredMessageRepository.findSentImmediatelyIsNullAndSentScheduledIsNullAndCreatedBefore(any(), any())).thenReturn(emptySlice);

        outboxHouseKeeping.deleteOldMessages();

        verify(deferredMessageRepository, times(2)).findSentImmediatelyBeforeOrSentScheduledBefore(any(), any());
        verify(deferredMessageRepository, times(2)).deleteAllById(any());
    }

    @Test
    void deleteOldMessages_UsesSentMessageRetentionDuration() {
        Slice<Long> slice = new SliceImpl<>(List.of(1L), Pageable.ofSize(10), false);
        when(deferredMessageRepository.findSentImmediatelyBeforeOrSentScheduledBefore(any(), any())).thenReturn(slice);
        Slice<Long> emptySlice = new SliceImpl<>(List.of());
        when(deferredMessageRepository.findSentImmediatelyIsNullAndSentScheduledIsNullAndCreatedBefore(any(), any())).thenReturn(emptySlice);

        ZonedDateTime beforeCall = ZonedDateTime.now().minus(Duration.ofDays(7));

        outboxHouseKeeping.deleteOldMessages();

        verify(deferredMessageRepository).findSentImmediatelyBeforeOrSentScheduledBefore(dateTimeCaptor.capture(), any());
        ZonedDateTime capturedDateTime = dateTimeCaptor.getValue();
        assertThat(capturedDateTime)
                .isAfterOrEqualTo(beforeCall)
                .isBeforeOrEqualTo(ZonedDateTime.now().minus(Duration.ofDays(7)));
    }

    @Test
    void deleteOldMessages_UsesUnsentMessageRetentionDuration() {
        Slice<Long> emptySlice = new SliceImpl<>(List.of());
        when(deferredMessageRepository.findSentImmediatelyBeforeOrSentScheduledBefore(any(), any())).thenReturn(emptySlice);
        Slice<Long> slice = new SliceImpl<>(List.of(1L), Pageable.ofSize(10), false);
        when(deferredMessageRepository.findSentImmediatelyIsNullAndSentScheduledIsNullAndCreatedBefore(any(), any())).thenReturn(slice);

        ZonedDateTime beforeCall = ZonedDateTime.now().minus(Duration.ofDays(30));

        outboxHouseKeeping.deleteOldMessages();

        verify(deferredMessageRepository).findSentImmediatelyIsNullAndSentScheduledIsNullAndCreatedBefore(dateTimeCaptor.capture(), any());
        ZonedDateTime capturedDateTime = dateTimeCaptor.getValue();
        assertThat(capturedDateTime)
                .isAfterOrEqualTo(beforeCall)
                .isBeforeOrEqualTo(ZonedDateTime.now().minus(Duration.ofDays(30)));
    }

    @Test
    void deleteOldMessages_UsesConfiguredPageSize() {
        when(config.getHouseKeepingPageSize()).thenReturn(25);
        outboxHouseKeeping = new OutboxHouseKeeping(deferredMessageRepository, config, transactionTemplate);

        Slice<Long> slice = new SliceImpl<>(List.of(1L), Pageable.ofSize(25), false);
        when(deferredMessageRepository.findSentImmediatelyBeforeOrSentScheduledBefore(any(), any())).thenReturn(slice);
        Slice<Long> emptySlice = new SliceImpl<>(List.of());
        when(deferredMessageRepository.findSentImmediatelyIsNullAndSentScheduledIsNullAndCreatedBefore(any(), any())).thenReturn(emptySlice);

        outboxHouseKeeping.deleteOldMessages();

        ArgumentCaptor<Pageable> pageableCaptor = ArgumentCaptor.forClass(Pageable.class);
        verify(deferredMessageRepository).findSentImmediatelyBeforeOrSentScheduledBefore(any(), pageableCaptor.capture());
        assertThat(pageableCaptor.getValue().getPageSize()).isEqualTo(25);
    }

    @Test
    void deleteOldMessages_ExecutesInSeparateTransactions() {
        Slice<Long> slice = new SliceImpl<>(List.of(1L, 2L), Pageable.ofSize(10), false);
        when(deferredMessageRepository.findSentImmediatelyBeforeOrSentScheduledBefore(any(), any())).thenReturn(slice);
        when(deferredMessageRepository.findSentImmediatelyIsNullAndSentScheduledIsNullAndCreatedBefore(any(), any())).thenReturn(slice);

        outboxHouseKeeping.deleteOldMessages();

        verify(transactionTemplate, times(2)).execute(any());
    }

    @Test
    void deleteOldMessages_WhenEmptyPagesInBetween_ThenStopsAtFirstEmptyPage() {
        Slice<Long> emptySlice = new SliceImpl<>(List.of(), Pageable.ofSize(10), false);
        when(deferredMessageRepository.findSentImmediatelyBeforeOrSentScheduledBefore(any(), any())).thenReturn(emptySlice);
        when(deferredMessageRepository.findSentImmediatelyIsNullAndSentScheduledIsNullAndCreatedBefore(any(), any())).thenReturn(emptySlice);

        outboxHouseKeeping.deleteOldMessages();

        verify(deferredMessageRepository, times(1)).findSentImmediatelyBeforeOrSentScheduledBefore(any(), any());
        verify(deferredMessageRepository, times(1)).findSentImmediatelyIsNullAndSentScheduledIsNullAndCreatedBefore(any(), any());
        verify(deferredMessageRepository, never()).deleteAllById(any());
    }
}
