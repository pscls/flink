package org.apache.flink.connector.rabbitmq2.source.reader;

import org.apache.flink.connector.rabbitmq2.source.common.RabbitMQMessageWrapper;
import org.apache.flink.streaming.connectors.rabbitmq.RMQDeserializationSchema;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * The collector for the messages received from rabbitmq. Deserialized receive their identifiers
 * through {@link #setMessageIdentifiers(String, long)} before they are collected through
 * {@link #collect(Object)}. Messages can be polled in order to be processed by the output.
 *
 * @param <T> The output type of the source.
 * @see RabbitMQMessageWrapper
 */
public class RabbitMQCollector<T> implements RMQDeserializationSchema.RMQCollector<T> {
    // Queue that holds the messages.
    private final BlockingQueue<RabbitMQMessageWrapper<T>> unpolledMessageQueue;
    // Identifiers of the next message that will be collected.
    private long deliveryTag;
    private String correlationId;

    private RabbitMQCollector(int capacity) {
        this.unpolledMessageQueue = new LinkedBlockingQueue<>(capacity);
    }

    public RabbitMQCollector() {
        this(Integer.MAX_VALUE);
    }

    /**
     *
     * @return boolean true if there are messages remaining in the collector.
     */
    public boolean hasUnpolledMessages() {
        return !unpolledMessageQueue.isEmpty();
    }

    /**
     * Poll a message from the collector.
     *
     * @return Message the polled message.
     */
    public RabbitMQMessageWrapper<T> pollMessage() {
        return unpolledMessageQueue.poll();
    }

    public BlockingQueue<RabbitMQMessageWrapper<T>> getMessageQueue() {
        return unpolledMessageQueue;
    }

    @Override
    public boolean setMessageIdentifiers(String correlationId, long deliveryTag) {
        this.correlationId = correlationId;
        this.deliveryTag = deliveryTag;

        return true;
    }

    @Override
    public void collect(T record) {
        unpolledMessageQueue.add(new RabbitMQMessageWrapper<>(deliveryTag, correlationId, record));
    }

    @Override
    public void close() {}
}
