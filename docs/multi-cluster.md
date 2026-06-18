# Multi-cluster support

The outbox supports sending messages to more than one Kafka cluster, following the same multi-cluster
configuration as [jeap-messaging](https://github.com/jeap-admin-ch/jeap-messaging) (clusters declared
under `jeap.messaging.kafka.cluster.<name>.*`).

For every configured cluster, `OutboxBeanRegistrar` registers a dedicated `TransactionalOutbox`,
`DeferredMessageSender` and serializer bean. The target cluster is stored per message in the
`cluster_name` column of `deferred_message`.

## Selecting a cluster

The default cluster's `TransactionalOutbox` is injected without a qualifier. To produce to a
non-default cluster, inject its outbox with a `@Qualifier` naming the cluster:

```java
@Autowired
private TransactionalOutbox transactionalOutbox;            // default cluster

@Autowired
@Qualifier("secondcluster")
private TransactionalOutbox transactionalOutboxSecondCluster;  // a non-default cluster
```

Each `TransactionalOutbox` stamps the message it persists with its own cluster name, and the relay
uses that name to choose the right Kafka template when sending.

## Unknown cluster fallback

If the relay finds a message whose `cluster_name` is not known in the current jEAP messaging
configuration, it sends that message to the **default producer cluster** instead (honouring any
configured default-producer-cluster override), and logs a debug entry. This makes the relay resilient
to cluster names that were valid when the message was stored but are no longer configured.

## Related

- [Sending messages](sending-messages.md)
- [Architecture](architecture.md)
- [jeap-messaging-outbox](../README.md)
