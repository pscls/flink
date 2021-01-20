package org.apache.flink.connector.rabbitmq2.source.reader;

import org.apache.flink.connector.rabbitmq2.source.split.RabbitMQPartitionSplit;
import org.apache.flink.connector.rabbitmq2.source.split.RabbitMQPartitionSplitSerializer;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.*;

public class PartitionSplitSerializerTest {

	private RabbitMQPartitionSplit getPartitionSplit() {

		String queueName = "exampleQueueName";
		List<String> ids = new ArrayList<>();
		for (int i = 0; i < 1000; i++) {
			ids.add(Integer.toString(i));
		}
		Set<String> correlationIds = new HashSet<>(ids);
		return new RabbitMQPartitionSplit(null, queueName, correlationIds);
	}

	@Test
	public void testSplitSerializer() {
		RabbitMQPartitionSplit split = getPartitionSplit();
		RabbitMQPartitionSplitSerializer serializer = new RabbitMQPartitionSplitSerializer();
		RabbitMQPartitionSplit deserializedSplit = null;
		try {
			byte[] serializedSplit = serializer.serialize(split);
			deserializedSplit = serializer.deserialize(0, serializedSplit);
		} catch (IOException e) {
			e.printStackTrace();
		}
		assertEquals(split, deserializedSplit);
	}
}
