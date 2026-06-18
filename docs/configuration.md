# Configuration reference

All outbox properties use the prefix `jeap.messaging.transactional-outbox`. They are bound to
`TransactionalOutboxConfigurationProperties`. Every property has a default, so the outbox works with no
configuration. Durations use ISO-8601 (`PT15S`, `P2D`) or Spring duration syntax.

```yaml
jeap:
  messaging:
    transactional-outbox:
      message-send-immediately-timeout: 15s
      scheduled-relay-enabled: true
      sent-message-retention-duration: P2D
```

## Delivery (immediate)

| Property                                | Default | Type     | Description                                                                                       |
|-----------------------------------------|---------|----------|---------------------------------------------------------------------------------------------------|
| `message-send-immediately-timeout`      | `PT15S` | Duration | Max time to wait for Kafka to finish sending when sending immediately after commit. Keep small so the request thread is not held too long if Kafka is slow |
| `message-send-immediately-max-block-time` | `PT5S` | Duration | Max time to wait for Kafka to start sending (producer `max.block.ms`) for immediate sends. Keep small |

## Delivery (scheduled / relay)

| Property                              | Default   | Type     | Description                                                                                  |
|---------------------------------------|-----------|----------|----------------------------------------------------------------------------------------------|
| `message-send-scheduled-timeout`      | `PT60S`   | Duration | Max time to wait for Kafka to finish sending in the relay. Choose large enough to relay even when Kafka is slow |
| `message-send-scheduled-max-block-time` | `PT15S` | Duration | Max time to wait for Kafka to start sending (producer `max.block.ms`) in the relay           |
| `scheduled-relay-enabled`             | `true`    | boolean  | Enable the background relay process. Disable to run dedicated relay instances elsewhere       |
| `poll-delay`                          | `PT2S`    | Duration | Delay between relay polls (also used as the scheduler fixed delay and the relay backoff)       |
| `continuous-relay-timeout`            | `PT5M`    | Duration | Max time the relay keeps sending without interruption before yielding. Bounds how long it holds the relay lock |
| `message-relay-batch-size`            | `5`       | int      | Max number of messages the relay reads and sends per batch                                    |

## Housekeeping & retention

| Property                            | Default       | Type     | Description                                                       |
|-------------------------------------|---------------|----------|-------------------------------------------------------------------|
| `house-keeping-schedule`            | `0 0 3 * * *` | Cron     | Cron expression for when the cleanup job runs                     |
| `house-keeping-page-size`           | `500`         | int      | Page size for housekeeping delete queries                         |
| `house-keeping-max-pages`           | `100000`      | int      | Max pages per run (`page-size * max-pages` = max deletes per kind per run) |
| `sent-message-retention-duration`   | `P2D`         | Duration | How long successfully sent messages are kept before deletion      |
| `unsent-message-retention-duration` | `P30D`        | Duration | How long not-yet-sent messages are kept before deletion           |

## Metrics

| Property                  | Default | Type     | Description                          |
|---------------------------|---------|----------|--------------------------------------|
| `metrics-update-interval` | `PT10S` | Duration | Interval between metrics-gauge updates |

## Related

- [Getting started](getting-started.md)
- [Sending messages](sending-messages.md)
- [Housekeeping & retention](housekeeping.md)
- [Metrics](metrics.md)
- [jeap-messaging-outbox](../README.md)
