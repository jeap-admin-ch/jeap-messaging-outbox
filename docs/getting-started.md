# Getting started

This page shows how to add jEAP Messaging outbox to a Spring Boot service and publish a Kafka message
inside a database transaction. The outbox builds on [jeap-messaging](https://github.com/jeap-admin-ch/jeap-messaging):
the service must already be a jEAP messaging producer (Kafka cluster configured, Avro message types,
producer contracts). For the bigger picture see [Architecture](architecture.md).

## 1. Add the dependency

```xml
<dependency>
    <groupId>ch.admin.bit.jeap</groupId>
    <artifactId>jeap-messaging-outbox</artifactId>
</dependency>
```

The version is managed by the jEAP Spring Boot parent. The module pulls in `jeap-messaging-avro` and
`jeap-messaging-infrastructure-kafka` together with Spring Data JPA. Auto-configuration wires up the
outbox automatically.

## 2. Create the database schema

The outbox stores messages in a `deferred_message` table and uses a `shedlock` table for scheduling.
Add the [DDL](database.md) to your service's database migration (e.g. Flyway). If the `shedlock`
table already exists (for instance because you also use `@IdempotentMessageHandler`), do not create
it twice.

The outbox declares its JPA entity via `@EntityScan` and its repository via `@EnableJpaRepositories`.
If your application uses JPA but does not declare an explicit `@EntityScan` / `@EnableJpaRepositories`,
add those annotations to your `@SpringBootApplication` class so Spring Boot's default scanning of your
own entities is preserved.

## 3. Configure (optional)

All outbox properties live under `jeap.messaging.transactional-outbox.*` and have sensible defaults.
The minimal setup needs no extra configuration. See the [Configuration reference](configuration.md).

## 4. Send a message

Inject `TransactionalOutbox` and call it from within a transaction. The message is persisted with your
business data and sent only after the transaction commits.

```java
@Service
@RequiredArgsConstructor
class DeclarationService {

    private final TransactionalOutbox outbox;
    private final DeclarationRepository declarationRepository;

    @Transactional
    public void createDeclaration(Declaration declaration) {
        // 1. change persistent business state
        declarationRepository.save(declaration);

        // 2. queue the event in the same transaction
        JmeDeclarationCreatedEvent event = JmeDeclarationCreatedEventBuilder.create()
                .idempotenceId(declaration.getId())
                .declarationIdReference(declaration.getId())
                .build();
        outbox.sendMessage(event, "jme-messaging-declaration-created");
    }
}
```

The send methods require an active transaction (`@Transactional(propagation = Propagation.MANDATORY)`);
calling them without one throws. `sendMessage(..)` sends immediately after commit; `sendMessageScheduled(..)`
hands the message to the background relay. See [Sending messages](sending-messages.md) for both modes
and for optional message keys and cluster qualifiers.

## Related

- [Architecture](architecture.md)
- [Sending messages](sending-messages.md)
- [Database schema & migrations](database.md)
- [Configuration reference](configuration.md)
- [jeap-messaging-outbox](../README.md)
