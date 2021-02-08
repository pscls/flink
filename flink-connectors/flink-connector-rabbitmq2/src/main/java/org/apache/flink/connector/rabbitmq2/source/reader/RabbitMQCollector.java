package org.apache.flink.connector.rabbitmq2.source.reader;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.rabbitmq2.source.common.Message;
import org.apache.flink.streaming.connectors.rabbitmq.RMQDeserializationSchema;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class RabbitMQCollector<T> implements RMQDeserializationSchema.RMQCollector<T> {
	private final BlockingQueue<Message<T>> unpolledMessageQueue;
	private long deliveryTag;
	private String correlationId;

	private RabbitMQCollector(int capacity) {
		this.unpolledMessageQueue = new LinkedBlockingQueue<>(capacity);
	}

	public RabbitMQCollector() {
		this(Integer.MAX_VALUE);
	}

	public boolean hasUnpolledMessages() {
		return !unpolledMessageQueue.isEmpty();
	}

	public int getNumberOfUnpolledMessages () {
		return unpolledMessageQueue.size();
	}

	// copied from old rmq connector
//	public void processMessage(Delivery delivery) throws IOException {
////		AMQP.BasicProperties properties = delivery.getProperties();
//		if (unpolledMessageQueue.remainingCapacity() == 0) {
//			return;
//		}
//		byte[] body = delivery.getBody();
//		Envelope envelope = delivery.getEnvelope();
//		long deliveryTag = envelope.getDeliveryTag();
//		T message = deliveryDeserializer.deserialize(body);
//		AMQP.BasicProperties properties = delivery.getProperties();
//		String correlationId = properties.getCorrelationId();
//
//		System.out.println("[Tag: "+ deliveryTag + "] " + message);
//		unpolledMessageQueue.add(new Message<>(deliveryTag, correlationId, message));
//	}

	public Message<T> pollMessage() {
		return unpolledMessageQueue.poll();
	}

	@VisibleForTesting
	public BlockingQueue<Message<T>> getMessageQueue() {
		return unpolledMessageQueue;
	}

	@Override
	public boolean setMessageIdentifiers(String correlationId, long deliveryTag) {
		this.correlationId = correlationId;
		this.deliveryTag = deliveryTag;

		return true;
	}

	public void setFallBackIdentifiers(String correlationId, long deliveryTag){
		this.correlationId = correlationId;
		this.deliveryTag = deliveryTag;
	}

	@Override
	public void collect(T record) {
		unpolledMessageQueue.add(new Message<>(deliveryTag, correlationId, record));
	}

	@Override
	public void close() {

	}
}
