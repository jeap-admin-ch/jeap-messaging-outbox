# jEAP Messaging outbox

jEAP Messaging outbox is an implementation of the
[Transactional outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html) for
jEAP Spring Boot services. It lets a service change persistent data and publish Kafka messages within
a single database transaction: messages are stored in an outbox table together with the business data
in the same transaction, and are only relayed to Kafka after the transaction commits. If the
transaction rolls back, the messages are never sent. This gives reliable, at-least-once message
delivery without distributed transactions (XA / 2PC), even when Kafka is temporarily unavailable.

It complements [jeap-messaging](https://github.com/jeap-admin-ch/jeap-messaging): messages are still
strongly-typed Avro events and commands, contracts are still enforced, and signing, encryption and
tracing carry over.

* Atomic "save data + queue message" in one local DB transaction
* Two delivery modes: send immediately after commit, or hand off to a scheduled background relay
* Background message relay with ShedLock-based locking for safe multi-instance operation
* Failed-message handling (query, mark for resend) for messages that fail for message-specific reasons
* Configurable housekeeping with separate retention for sent and unsent messages
* Multi-cluster support and Micrometer metrics for operational monitoring

## Documentation

Start with [Getting started](docs/getting-started.md), then follow the links below.

| Topic                                                          | File                                                       |
|----------------------------------------------------------------|------------------------------------------------------------|
| Getting started (add the dependency, send a message)           | [docs/getting-started.md](docs/getting-started.md)         |
| Architecture & the relay flow                                  | [docs/architecture.md](docs/architecture.md)               |
| Sending messages (`TransactionalOutbox` API, delivery modes)   | [docs/sending-messages.md](docs/sending-messages.md)       |
| Database schema & migrations                                   | [docs/database.md](docs/database.md)                       |
| Failure handling & failed-message operations                   | [docs/failure-handling.md](docs/failure-handling.md)       |
| Housekeeping & retention                                       | [docs/housekeeping.md](docs/housekeeping.md)               |
| Metrics                                                        | [docs/metrics.md](docs/metrics.md)                         |
| Multi-cluster support                                          | [docs/multi-cluster.md](docs/multi-cluster.md)             |
| Configuration reference (`jeap.messaging.transactional-outbox.*`) | [docs/configuration.md](docs/configuration.md)          |

## Modules

Group id for all modules is `ch.admin.bit.jeap`; the version is managed by the jEAP Spring Boot parent.

| Module                       | Purpose                                                                        |
|------------------------------|--------------------------------------------------------------------------------|
| `jeap-messaging-outbox`      | The outbox implementation; the artifact consumers depend on                    |
| `jeap-messaging-outbox-test` | Internal integration-test module (embedded Kafka, H2, Flyway); not a consumer API |

## Changes

This library is versioned using [Semantic Versioning](http://semver.org/) and all changes are documented in
[CHANGELOG.md](./CHANGELOG.md) following the format defined in [Keep a Changelog](http://keepachangelog.com/).

## Note

This repository is part the open source distribution of jEAP. See [github.com/jeap-admin-ch/jeap](https://github.com/jeap-admin-ch/jeap)
for more information.

## License

This repository is Open Source Software licensed under the [Apache License 2.0](./LICENSE).
