package org.apache.flink.connector.rabbitmq2.source.enumerator;


import org.apache.flink.connector.rabbitmq2.source.split.RabbitMQPartitionSplit;

import java.util.Map;
import java.util.List;

public class RabbitMQSourceEnumState {
	private final Map<Integer, List<RabbitMQPartitionSplit>> currentAssignment;

	RabbitMQSourceEnumState(Map<Integer, List<RabbitMQPartitionSplit>> currentAssignment) {
		this.currentAssignment = currentAssignment;
	}

	public Map<Integer, List<RabbitMQPartitionSplit>> getCurrentAssignment() {
		return currentAssignment;
	}
}
