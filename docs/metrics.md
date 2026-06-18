# Metrics

The outbox exposes Micrometer metrics so its state and activity can be monitored in production. Metrics
are only recorded when a `MeterRegistry` Spring bean is available — for example by adding the
`jeap-spring-boot-monitoring-starter`. Without a `MeterRegistry`, no metrics are collected and the
outbox still works.

The gauges are refreshed on a fixed interval by `OutboxMetricsUpdateScheduler` (every
`metrics-update-interval`, default 10 seconds).

## Metrics

| Name                                   | Type    | Description                                                                                  |
|----------------------------------------|---------|----------------------------------------------------------------------------------------------|
| `outbox_messages_ready_to_be_sent_count` | Gauge | Number of messages ready to be relayed (the outbox "lag")                                     |
| `outbox_messages_failed_count`         | Gauge   | Number of messages in state `failed`, split by tag `resend_status`                            |
| `outbox_messages_post_total`           | Counter | Number of messages posted to the outbox, split by tags `delivery_type` and `tx_status`        |
| `outbox_messages_transmit`             | Timer   | Count and duration of messages actually transmitted to Kafka, tagged by `delivery_type`       |
| `outbox_messages_ready_to_be_sent_query` | Timer | Count and duration of the query that fetches messages ready to be sent                         |

The `outbox_messages_transmit` timer is exported by Micrometer as
`outbox_messages_transmit_seconds_count`, `_sum` and `_max`.

## Tags

| Tag             | Values                                  | Used by                                              |
|-----------------|-----------------------------------------|------------------------------------------------------|
| `delivery_type` | `immediate`, `scheduled`                | `outbox_messages_post_total`, `outbox_messages_transmit` |
| `tx_status`     | `committed`, `rolled_back`, `unknown`   | `outbox_messages_post_total`                          |
| `resend_status` | `resend_enabled`, `resend_disabled`     | `outbox_messages_failed_count`                        |

`tx_status` reflects the outcome of the transaction in which the message was posted: `committed` and
`rolled_back` are recorded via a transaction synchronization; `unknown` is used when no transaction
synchronization is active when counting the post.

## What to monitor

- `outbox_messages_ready_to_be_sent_count` rising steadily indicates the relay is falling behind (for
  example Kafka problems or the relay being disabled).
- `outbox_messages_failed_count` (with `resend_disabled`) above zero means messages need operator
  attention — see [Failure handling](failure-handling.md).
- `outbox_messages_post_total{tx_status="rolled_back"}` indicates messages that were posted in
  transactions that later rolled back and were therefore correctly not sent.

## Related

- [Failure handling](failure-handling.md)
- [Configuration reference](configuration.md)
- [jeap-messaging-outbox](../README.md)
