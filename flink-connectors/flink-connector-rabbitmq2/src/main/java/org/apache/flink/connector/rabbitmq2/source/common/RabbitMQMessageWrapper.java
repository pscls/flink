package org.apache.flink.connector.rabbitmq2.source.common;

/**
 * A wrapper class for the message received from rabbitmq that holds the deserialized message, the
 * delivery tag and the correlation id.
 *
 * @param <T> The type of the message to hold.
 */
public class RabbitMQMessageWrapper<T> {
    private final long deliveryTag;
    private final String correlationId;
    private final T message;

    public RabbitMQMessageWrapper(long deliveryTag, String correlationId, T message) {
        this.deliveryTag = deliveryTag;
        this.correlationId = correlationId;
        this.message = message;
    }

    public RabbitMQMessageWrapper(T message, long deliveryTag) {
        this(deliveryTag, null, message);
    }

    public RabbitMQMessageWrapper(long deliveryTag, String correlationId) {
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
