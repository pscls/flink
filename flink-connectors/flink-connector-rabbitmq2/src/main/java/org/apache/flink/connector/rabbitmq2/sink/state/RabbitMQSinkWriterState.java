package org.apache.flink.connector.rabbitmq2.sink.state;

import org.apache.flink.connector.rabbitmq2.sink.SinkMessage;

import java.io.Serializable;
import java.util.List;

/** TODO. */
public class RabbitMQSinkWriterState<T> implements Serializable {
    private final List<SinkMessage<T>> outstandingMessages;

    public RabbitMQSinkWriterState(List<SinkMessage<T>> outstandingMessages) {
        this.outstandingMessages = outstandingMessages;
    }

    public List<SinkMessage<T>> getOutstandingMessages() {
        return outstandingMessages;
    }
}
