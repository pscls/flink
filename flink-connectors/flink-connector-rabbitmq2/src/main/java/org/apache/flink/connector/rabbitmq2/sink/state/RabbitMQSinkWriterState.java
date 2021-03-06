package org.apache.flink.connector.rabbitmq2.sink.state;

import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.rabbitmq2.sink.SinkMessage;
import org.apache.flink.connector.rabbitmq2.sink.writer.specalized.RabbitMQSinkWriterAtLeastOnce;
import org.apache.flink.connector.rabbitmq2.sink.writer.specalized.RabbitMQSinkWriterExactlyOnce;

import java.util.List;

/**
 * The state of a {@link SinkWriter} implementation. Contains {@code outstandingMessages} that could
 * not be delivered in a checkpoint. Used in the {@link RabbitMQSinkWriterAtLeastOnce} and {@link
 * RabbitMQSinkWriterExactlyOnce} implementations.
 */
public class RabbitMQSinkWriterState<T> {
    private final List<SinkMessage<T>> outstandingMessages;

    public RabbitMQSinkWriterState(List<SinkMessage<T>> outstandingMessages) {
        this.outstandingMessages = outstandingMessages;
    }

    public List<SinkMessage<T>> getOutstandingMessages() {
        return outstandingMessages;
    }
}
