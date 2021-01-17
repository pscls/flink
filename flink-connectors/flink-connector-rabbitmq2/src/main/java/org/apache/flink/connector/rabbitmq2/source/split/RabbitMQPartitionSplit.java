package org.apache.flink.connector.rabbitmq2.source.split;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class RabbitMQPartitionSplit implements SourceSplit, Serializable {

	private final String rmqQueueName;
	private Set<String> correlationIds;

	public RabbitMQPartitionSplit(String rmqQueueName) {
		this(rmqQueueName, new HashSet<>());
	}

	public RabbitMQPartitionSplit(String rmqQueueName, Set<String> correlationIds) {
		this.rmqQueueName = rmqQueueName;
		this.correlationIds = correlationIds;
	}

	public Set<String> getCorrelationIds() {
		return correlationIds;
	}

	public String getQueueName() { return rmqQueueName; }

	public void setCorrelationIds(Set<String> newCorrelationIds) {
		correlationIds = newCorrelationIds;
	}

	@Override
	public String splitId() {
		return correlationIds.toString(); // TODO: find something better
	}

	@Override
	public boolean equals(Object other) {
		if (this == other)
			return true;
		if (other == null)
			return false;
		if (getClass() != other.getClass())
			return false;
		RabbitMQPartitionSplit otherSplit = (RabbitMQPartitionSplit) other;
		return this.getQueueName().equals(otherSplit.getQueueName()) &&
			this.getCorrelationIds().equals(otherSplit.getCorrelationIds());
	}
}
