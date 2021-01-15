package org.apache.flink.connector.rabbitmq2.source.reader;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;


public class RabbitMQCollector<T> {
	private final Queue<Tuple2<Long, T>> unpolledMessageQueue;
	private final DeserializationSchema<T> deliveryDeserializer;
	private final boolean useCorrelationIds;

	public RabbitMQCollector(DeserializationSchema<T> deliveryDeserializer, boolean useCorrelationIds) {
		this.deliveryDeserializer = deliveryDeserializer;
		this.unpolledMessageQueue = new LinkedList<>();
		this.useCorrelationIds = useCorrelationIds;
	}

	private class MessageID {
		public final long deliveryTag;
		public final Optional<String> correlationId;
		public MessageID(long deliveryTag, Optional<String> correlationId) {
			this.deliveryTag = deliveryTag;
			this.correlationId = correlationId;
		}
	}


	public boolean hasUnpolledMessages() {
		return !unpolledMessageQueue.isEmpty();
	}

	public int getNumberOfUnpolledMessages () {
		return unpolledMessageQueue.size();
	}

	// copied from old rmq connector
	public boolean processMessage(Delivery delivery, Set<String> correlationIds)
		throws IOException {
		AMQP.BasicProperties properties = delivery.getProperties();


		byte[] body = delivery.getBody();
		Envelope envelope = delivery.getEnvelope();
		long deliveryTag = envelope.getDeliveryTag();
		String correlationId = properties.getCorrelationId();
		// check for acknowledge mode, only execute if its not auto ack
		if (useCorrelationIds) {
			Preconditions.checkNotNull(correlationId, "RabbitMQ source was instantiated " +
				"with usesCorrelationId set to true yet we couldn't extract the correlation id from it !");
			if (!correlationIds.add(correlationId)) {
				return false;
			}
		}
//		collector.setFallBackIdentifiers(properties.getCorrelationId(), envelope.getDeliveryTag());
		T message = deliveryDeserializer.deserialize(body);
		System.out.println("[Tag: "+ deliveryTag + "] " + message);
		unpolledMessageQueue.add(new Tuple2<>(deliveryTag, message));
		return true;
	}

	public Tuple2<Long, T> getMessage() {
		return unpolledMessageQueue.poll();
	}
}
