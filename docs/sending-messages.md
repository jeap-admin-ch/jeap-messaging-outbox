# Sending messages

Messages are sent through the `TransactionalOutbox` bean. All send methods require an active
transaction (they are annotated `@Transactional(propagation = Propagation.MANDATORY)`) and validate
the message's producer contract before persisting it. The message is stored in the same transaction
as your business data and only delivered after that transaction commits.

## API

```java
public class TransactionalOutbox {
    void sendMessage(Message message, String topic);
    void sendMessage(Message message, Object key, String topic);
    void sendMessageScheduled(Message message, String topic);
    void sendMessageScheduled(Message message, Object key, String topic);
}
```

`message` is a jEAP messaging `Message` (an Avro event or command). The optional `key` is the Kafka
message key (an Avro `AvroMessageKey`); it is serialized and stored alongside the message. The `topic`
is the destination Kafka topic.

```java
@Transactional
public void publish(SomeEvent event, SomeKey key) {
    outbox.sendMessage(event, key, "some-topic");
}
```

## Delivery modes

| Method                  | When it is sent                                          | Use when                                                              |
|-------------------------|----------------------------------------------------------|-----------------------------------------------------------------------|
| `sendMessage(..)`       | Immediately after the surrounding transaction commits, in the caller's thread | Default choice; lowest latency, behaves most like a normal send       |
| `sendMessageScheduled(..)` | Later, by the background relay process                | When you do not want the request thread to wait on Kafka              |

### Immediate delivery (`sendMessage`)

Pros: low latency (sent right after commit) and horizontal scalability (each instance sends its own
messages in its own thread; the database is the limiting factor). Con: the request thread is held for
the duration of the send, which matters if Kafka is unavailable or slow — tune
`message-send-immediately-timeout` and `message-send-immediately-max-block-time` to bound this.

If the immediate send fails, it is abandoned and the message is left in the table for the background
relay to deliver later. Because of this, **at least one relay process must always be running**, even
when all messages are sent with `sendMessage(..)`.

### Scheduled delivery (`sendMessageScheduled`)

Pros: the request thread is not burdened with the actual send, which helps when Kafka is slow. Cons:
higher latency (the relay polls only every `poll-delay`, and messages are sent in insertion order, so
a new message waits behind older unsent ones) and limited throughput (only one relay sends at a time,
no parallelism).

## Choosing the cluster

In a [multi-cluster](multi-cluster.md) setup there is one `TransactionalOutbox` bean per Kafka cluster.
Inject the non-default cluster's outbox with a `@Qualifier`:

```java
@Autowired
@Qualifier("secondcluster")
private TransactionalOutbox outbox;
```

The default cluster's `TransactionalOutbox` needs no qualifier.

## Related

- [Architecture](architecture.md)
- [Failure handling](failure-handling.md)
- [Configuration reference](configuration.md)
- [Multi-cluster support](multi-cluster.md)
- [jeap-messaging-outbox](../README.md)
