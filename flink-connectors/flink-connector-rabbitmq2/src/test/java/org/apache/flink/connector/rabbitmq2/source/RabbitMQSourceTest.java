package org.apache.flink.connector.rabbitmq2.source;

import org.apache.flink.connector.rabbitmq2.source.common.ConsistencyMode;
import org.apache.flink.connector.rabbitmq2.source.reader.RabbitMQCollector;

import org.apache.flink.streaming.connectors.rabbitmq.RMQDeserializationSchema;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import org.junit.Test;
import org.mockito.Mockito;

public class RabbitMQSourceTest {
	
	@Test
	public void testAtMostOnce() {
//		RMQConnectionConfig connectionConfig = Mockito.mock(RMQConnectionConfig.class);
//		String queueName = "Queue";
//		DeserializationSchema<String> deserializationSchema = Mockito.mock(DeserializationSchema.class);
//		RabbitMQSource<String> source = new RabbitMQSource<>(connectionConfig, queueName, deserializationSchema,
//			ConsistencyMode.AT_MOST_ONCE);

	}
}
