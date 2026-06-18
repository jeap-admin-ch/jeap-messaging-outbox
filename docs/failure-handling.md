# Failure handling

The outbox distinguishes two kinds of send failures: transient failures (Kafka unavailable or slow)
and message-specific failures (something is wrong with the message itself).

## Transient failures (Kafka down or slow)

These are retried indefinitely.

- **Immediate delivery**: if a `sendMessage(..)` message cannot be delivered after commit, the
  immediate send is abandoned and the message is left in the table for the relay to deliver later.
- **Scheduled delivery / relay**: if the relay cannot send the current batch
  (`message-relay-batch-size`), it aborts the batch, pauses for `poll-delay`, and tries again. This is
  effectively an unbounded retry while Kafka is unavailable or too slow.

## Message-specific failures (failed messages)

If a send fails because of the message itself — not because Kafka is down — the message is marked
`failed` and ignored by the relay, so it cannot block delivery of all other messages. The failure
reasons (`SendFailureReason`) that mark a message as failed are:

| Reason                  | Cause                                                          |
|-------------------------|---------------------------------------------------------------|
| `INVALID_TOPIC`         | The target topic does not exist                               |
| `UNAUTHORIZED_ON_TOPIC` | No authorization on the topic                                 |
| `MESSAGE_TOO_LARGE`     | The message exceeds the topic / cluster size limit            |

Any other (general) error is treated as transient and retried; it does not mark the message as failed.

A failed message usually means an interrupted business process. **Using the outbox therefore requires
a defined operational procedure for handling failed messages**, and the [metrics](metrics.md) should
be monitored so DevOps can react.

## Operations on failed messages

`TransactionalOutbox` exposes methods to query and re-enable failed messages:

| Method                                                                   | Purpose                                                            |
|--------------------------------------------------------------------------|-------------------------------------------------------------------|
| `countFailedMessages(boolean resend)`                                    | Count failed messages, optionally restricted to those marked for resend |
| `countFailedMessages(ZonedDateTime from, ZonedDateTime before, boolean resend)` | Count failed messages within a time interval               |
| `findFailedMessages(ZonedDateTime from, ZonedDateTime before, boolean resend, int max)` | Find failed messages in a time interval (ordered by id)    |
| `findFailedMessages(long afterId, ZonedDateTime before, boolean resend, int max)` | Find failed messages after a given id (for paging)        |
| `resendMessageScheduled(long id)`                                        | Make a failed message available to the relay again (initiate a resend) |

A resend only succeeds once the root cause has been fixed (for example a missing topic authorization
has been granted). Re-enabling a message via `resendMessageScheduled` clears its failed state so the
relay will pick it up again.

## Related

- [Sending messages](sending-messages.md)
- [Metrics](metrics.md)
- [Configuration reference](configuration.md)
- [jeap-messaging-outbox](../README.md)
