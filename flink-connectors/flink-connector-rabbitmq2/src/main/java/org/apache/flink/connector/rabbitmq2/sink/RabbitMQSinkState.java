package org.apache.flink.connector.rabbitmq2.sink;

import java.util.Collection;

public class RabbitMQSinkState {
    private final Collection<byte[]> outstandingMessages;
    public RabbitMQSinkState(Collection<byte[]> outstandingMessages) {
        this.outstandingMessages = outstandingMessages;
    }

    public Collection<byte[]> getOutstandingMessages() { return outstandingMessages; }
}
