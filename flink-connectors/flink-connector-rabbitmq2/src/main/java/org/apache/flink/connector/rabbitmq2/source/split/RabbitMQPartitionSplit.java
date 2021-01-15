package org.apache.flink.connector.rabbitmq2.source.split;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.util.HashSet;
import java.util.Set;

public class RabbitMQPartitionSplit implements SourceSplit {

	private final Set<String> correlationIds;

	public RabbitMQPartitionSplit() {
		this(new HashSet<>());
	}

	public RabbitMQPartitionSplit(Set<String> correlationIds) {
		this.correlationIds = correlationIds;
	}

	public Set<String> getCorrelationIds() {
		return correlationIds;
	}

	@Override
	public String splitId() {
		return correlationIds.toString(); // TODO: find something better
	}
}
