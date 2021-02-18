package org.apache.flink.connector.rabbitmq2.source.common;

/** TODO. */
public class Message<T> {
    private final long deliveryTag;
    private final String correlationId;
    private final T message;

    public Message(long deliveryTag, String correlationId, T message) {
        this.deliveryTag = deliveryTag;
        this.correlationId = correlationId;
        this.message = message;
    }

    public Message(T message, long deliveryTag) {
        this(deliveryTag, null, message);
    }

    public Message(long deliveryTag, String correlationId) {
        this(deliveryTag, correlationId, null);
    }

    public long getDeliveryTag() {
        return deliveryTag;
    }

    public T getMessage() {
        return message;
    }

    public String getCorrelationId() {
        return correlationId;
    }
}
