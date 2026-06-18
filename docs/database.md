# Database schema & migrations

The outbox requires its own tables: `deferred_message` for the stored messages and `shedlock` for
scheduler locking. Add the DDL to your service's own database migration (for example Flyway). The
outbox only ships this DDL as a test migration within the repository
(`jeap-messaging-outbox-test/src/test/resources/db/migration/common/`); the consuming service owns the
production migration.

If the `shedlock` table already exists — for instance because the service also uses
`@IdempotentMessageHandler` — do not create it a second time.

## DDL (PostgreSQL)

```sql
CREATE SEQUENCE deferred_message_sequence START WITH 1 INCREMENT 1;

CREATE TABLE deferred_message
(
    id                     bigint PRIMARY KEY,
    message                bytea                    NOT NULL,
    "key"                  bytea,
    cluster_name           varchar,
    topic                  varchar                  NOT NULL,
    message_id             varchar                  NOT NULL,
    message_idempotence_id varchar                  NOT NULL,
    message_type_name      varchar                  NOT NULL,
    message_type_version   varchar,
    created                timestamp with time zone NOT NULL,
    send_immediately       boolean,
    schedule_after         timestamp with time zone,
    sent_immediately       timestamp with time zone,
    sent_scheduled         timestamp with time zone,
    failed                 timestamp with time zone,
    fail_reason            varchar,
    resend                 boolean DEFAULT FALSE,
    trace_id_high          bigint,
    trace_id               bigint,
    span_id                bigint,
    parent_span_id         bigint,
    trace_id_string        varchar,
    sampled                boolean
);

CREATE INDEX deferred_message_created ON deferred_message (created);
CREATE INDEX deferred_message_send_immediately ON deferred_message (send_immediately);
CREATE INDEX deferred_message_schedule_after ON deferred_message (schedule_after);
CREATE INDEX deferred_message_sent_immediately ON deferred_message (sent_immediately);
CREATE INDEX deferred_message_sent_scheduled ON deferred_message (sent_scheduled);
CREATE INDEX deferred_message_failed ON deferred_message (failed);
CREATE INDEX deferred_message_resend ON deferred_message (resend);

CREATE TABLE shedlock
(
    name       VARCHAR(64)  NOT NULL,
    lock_until TIMESTAMP    NOT NULL,
    locked_at  TIMESTAMP    NOT NULL,
    locked_by  VARCHAR(255) NOT NULL,
    PRIMARY KEY (name)
);
```

## `deferred_message` columns

| Column                   | Meaning                                                                                    |
|--------------------------|--------------------------------------------------------------------------------------------|
| `message`                | The serialized Avro Kafka message bytes                                                     |
| `key`                    | The serialized Kafka message key (nullable)                                                 |
| `cluster_name`           | Target Kafka cluster; unknown clusters fall back to the default producer cluster           |
| `topic`                  | Destination Kafka topic                                                                     |
| `message_id` / `message_idempotence_id` | The message identity, used for logging and traceability                     |
| `message_type_name` / `message_type_version` | The message type and its generated version                            |
| `created`                | When the message was put into the outbox                                                   |
| `send_immediately`       | Whether immediate (`true`) or scheduled (`false`) delivery was requested                    |
| `schedule_after`         | Earliest time the relay should pick the message up (set for immediate messages before commit) |
| `sent_immediately`       | Timestamp of a successful immediate send (null until sent)                                  |
| `sent_scheduled`         | Timestamp of a successful relay send (null until sent)                                      |
| `failed` / `fail_reason` | When and why the message was marked failed (see [Failure handling](failure-handling.md))   |
| `resend`                 | Whether a failed message was re-enabled for the relay                                       |
| `trace_*` / `sampled`    | The captured trace context, restored when the message is relayed so the send span joins the original trace |

A message counts as *ready to be sent* once it is past its `schedule_after`, not yet sent (`sent_immediately`
and `sent_scheduled` both null) and not failed.

## Related

- [Getting started](getting-started.md)
- [Housekeeping & retention](housekeeping.md)
- [Failure handling](failure-handling.md)
- [jeap-messaging-outbox](../README.md)
