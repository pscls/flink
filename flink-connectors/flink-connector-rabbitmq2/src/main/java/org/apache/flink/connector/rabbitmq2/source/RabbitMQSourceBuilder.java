package org.apache.flink.connector.rabbitmq2.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

public class RabbitMQSourceBuilder<OUT> {

	public RabbitMQSource<OUT> build(RMQConnectionConfig connectionConfig, String queueName, DeserializationSchema<OUT> deliveryDeserializer) {
		return new RabbitMQSource<>(connectionConfig, queueName, deliveryDeserializer);
	}
}
