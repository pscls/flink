package org.apache.flink.connector.rabbitmq2.source.common;

public class Message<T> {
	public final long deliveryTag;
	public final String correlationId;
	public final T message;

	public Message(T message, long deliveryTag, String correlationId) {
		this.deliveryTag = deliveryTag;
		this.correlationId = correlationId;
		this.message = message;
	}

	public Message(T message, long deliveryTag) {
		this(message, deliveryTag, null);
	}

	public Message(long deliveryTag, String correlationId) {
		this(null, deliveryTag, null);
	}

	public long getDeliveryTag() {
		return deliveryTag;
	}

	public String getCorrelationId() {
		return correlationId;
	}
}
