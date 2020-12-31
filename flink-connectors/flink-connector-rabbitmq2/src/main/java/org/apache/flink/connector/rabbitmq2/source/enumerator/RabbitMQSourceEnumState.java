package org.apache.flink.connector.rabbitmq2.source.enumerator;


import org.apache.flink.connector.rabbitmq2.source.split.RabbitMQPartitionSplit;

import java.util.Map;
import java.util.Set;

public class RabbitMQSourceEnumState {
	private final Map<Integer, Set<RabbitMQPartitionSplit>> currentAssignment;

	RabbitMQSourceEnumState(Map<Integer, Set<RabbitMQPartitionSplit>> currentAssignment) {
		this.currentAssignment = currentAssignment;
	}

	public Map<Integer, Set<RabbitMQPartitionSplit>> getCurrentAssignment() {
		return currentAssignment;
	}
}
