package ch.admin.bit.jeap.messaging.transactionaloutbox.outbox;

import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.logstash.logback.argument.StructuredArgument;
import tools.jackson.core.JsonGenerator;

import java.io.IOException;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class DeferredMessageLogArgument implements StructuredArgument {

    @NonNull
    private final DeferredMessage deferredMessage;

    public static DeferredMessageLogArgument from(DeferredMessage deferredMessage) {
        return new DeferredMessageLogArgument(deferredMessage);
    }

    @Override
    public void writeTo(JsonGenerator generator) {
        generator.writeNumberProperty("deferredMessageId", deferredMessage.getId());
        generator.writeStringProperty("deferredMessageTopic", deferredMessage.getTopic());
        generator.writeStringProperty("deferredMessageCreated",deferredMessage.getCreated().toString());
        generator.writeBooleanProperty("deferredMessageSendImmediately",deferredMessage.isSendImmediately());
        generator.writeStringProperty("messageId", deferredMessage.getMessageId());
        generator.writeStringProperty("messageIdempotenceId", deferredMessage.getMessageIdempotenceId());
        generator.writeStringProperty("messageType", deferredMessage.getMessageTypeName());
    }

    @Override
    public String toString() {
        return String.format("deferred message (deferredMessageId=%s)", deferredMessage.getId());
    }
}
