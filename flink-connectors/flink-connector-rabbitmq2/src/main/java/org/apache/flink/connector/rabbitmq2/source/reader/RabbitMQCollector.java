package org.apache.flink.connector.rabbitmq2.source.reader;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;

import jdk.nashorn.internal.ir.Block;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.rabbitmq2.source.common.Message;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class RabbitMQCollector<T> {
	private final BlockingQueue<Message<T>> unpolledMessageQueue;
	private final DeserializationSchema<T> deliveryDeserializer;

	private RabbitMQCollector(DeserializationSchema<T> deliveryDeserializer, int capacity) {
		this.deliveryDeserializer = deliveryDeserializer;
		this.unpolledMessageQueue = new LinkedBlockingQueue<>();
	}

	public RabbitMQCollector(DeserializationSchema<T> deliveryDeserializer) {
		this(deliveryDeserializer, Integer.MAX_VALUE);
	}

	public boolean hasUnpolledMessages() {
		return !unpolledMessageQueue.isEmpty();
	}

	public int getNumberOfUnpolledMessages () {
		return unpolledMessageQueue.size();
	}

	// copied from old rmq connector
	public void processMessage(Delivery delivery) throws IOException {
//		AMQP.BasicProperties properties = delivery.getProperties();
		if (unpolledMessageQueue.remainingCapacity() == 0) {
			return;
		}
		byte[] body = delivery.getBody();
		Envelope envelope = delivery.getEnvelope();
		long deliveryTag = envelope.getDeliveryTag();
		T message = deliveryDeserializer.deserialize(body);
		AMQP.BasicProperties properties = delivery.getProperties();
		String correlationId = properties.getCorrelationId();

		System.out.println("[Tag: "+ deliveryTag + "] " + message);
		unpolledMessageQueue.add(new Message<>(deliveryTag, correlationId, message));
	}

	public Message<T> pollMessage() {
		return unpolledMessageQueue.poll();
	}

	@VisibleForTesting
	public BlockingQueue<Message<T>> getMessageQueue() {
		return unpolledMessageQueue;
	}
}
