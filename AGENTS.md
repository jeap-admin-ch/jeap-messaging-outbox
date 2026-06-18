# AGENTS.md

Guidance for AI coding agents working **in this repository**. For how to *use* the library in a
consuming service, read [README.md](README.md) and the [docs/](docs/) folder instead.

## Project

jEAP Messaging outbox is a Maven library implementing the Transactional Outbox pattern for jEAP
Spring Boot services. It persists outgoing Kafka messages in a `deferred_message` database table in
the same transaction as the business data and relays them to Kafka only after the transaction
commits. It builds on [jeap-messaging](https://github.com/jeap-admin-ch/jeap-messaging) (Avro
message types, contracts, signing, tracing) and is delivered via Spring Boot auto-configuration.

## Repository layout

```
pom.xml                          # Parent POM (packaging=pom); declares the modules below
jeap-messaging-outbox/           # The library: outbox API, relay, scheduling, JPA, metrics, auto-config
jeap-messaging-outbox-test/      # Internal integration tests (embedded Kafka, H2, Flyway); not published API
Jenkinsfile, publiccode.yml, CHANGELOG.md, LICENSE
```

Key packages under `ch.admin.bit.jeap.messaging.transactionaloutbox`:

- `outbox` — `TransactionalOutbox` (public API), `MessageRelay`, `OutboxHouseKeeping`,
  `DeferredMessage` entity, repositories, `SendFailureReason`, `OutboxConfig` (auto-configuration root)
- `transaction` — `TxSyncAfterCommitMessageSender` (immediate send via transaction synchronization)
- `scheduling` — `MessageRelayScheduler`, `OutboxHouseKeepingScheduler`, `OutboxMetricsUpdateScheduler`
- `messaging` — `KafkaDeferredMessageSender` (byte-array Kafka templates, signing, tracing)
- `jpa` — Spring Data JPA repository implementation
- `metrics` — `MicrometerOutboxMetrics`
- `spring` — `OutboxBeanRegistrar` registering per-cluster outbox beans
- `config` — `TransactionalOutboxConfigurationProperties` (`jeap.messaging.transactional-outbox.*`)

## Build & test

```bash
./mvnw verify                                   # full build incl. integration tests
./mvnw -pl jeap-messaging-outbox test
./mvnw -pl jeap-messaging-outbox-test verify    # integration tests (failsafe)
```

- Parent: `ch.admin.bit.jeap:jeap-internal-spring-boot-parent` (Spring Boot aligned).
- Integration tests use `@EmbeddedKafka` via `KafkaIntegrationTestBase`
  (`jeap-messaging-infrastructure-kafka-test`), H2 and Flyway. Every feature must have integration tests.

## jEAP conventions

- Configuration properties use the prefix `jeap.messaging.transactional-outbox.*`. The properties
  bean is named `txOutboxConfigProps` and is referenced from scheduling SpEL expressions — do not
  rename it without updating `MessageRelayScheduler` / `OutboxHouseKeepingScheduler`.
- Auto-configuration is registered in
  `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`.
- The outbox registers one `TransactionalOutbox`, `DeferredMessageSender` and serializer bean per
  configured Kafka cluster (`OutboxBeanRegistrar`); the non-default cluster beans are reached with a
  `@Qualifier("<clusterName>")`.
- Messages are stored as already-serialized Avro byte arrays, so the relay can send message types
  whose Java binding it does not have on the classpath.
- The DDL lives only as a Flyway test migration in this repo
  (`jeap-messaging-outbox-test/src/test/resources/db/migration/common/`). Consuming services own
  their own migration; keep [docs/database.md](docs/database.md) in sync with that DDL.

## Docs

When changing public behaviour, update the matching focused file under [docs/](docs/) (one topic per
file) and the documentation index in the README. Ground every documented fact in the source; the
Confluence page may be outdated (e.g. property defaults).

## Versioning

- Semantic Versioning; all changes documented in [CHANGELOG.md](./CHANGELOG.md) (Keep a Changelog format).
- `setPomVersions.sh` updates the version across all module POMs. Keep the `-SNAPSHOT` postfix on
  feature branches (CI removes it when releasing); do not use it in CHANGELOG / publiccode.yml.
- The `jeap-messaging.version` property tracks the upstream jeap-messaging dependency and is bumped
  independently from this library's own version.
- Use the JIRA ID from the branch name as the commit-message prefix (e.g. `JEAP-1234 Add feature X`).
- When bumping the version, also update the changelog and the version/date in `publiccode.yml`.
