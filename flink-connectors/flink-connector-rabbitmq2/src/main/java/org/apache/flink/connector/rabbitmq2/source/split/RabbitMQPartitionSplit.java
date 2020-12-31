package org.apache.flink.connector.rabbitmq2.source.split;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

public class RabbitMQPartitionSplit implements SourceSplit {

	private final RMQConnectionConfig connectionConfig;

	public RabbitMQPartitionSplit(
		RMQConnectionConfig connectionConfig) {
		this.connectionConfig = connectionConfig;
	}

	public RMQConnectionConfig getConnectionConfig() {
		return connectionConfig;
	}

	@Override
	public String splitId() {
		return connectionConfig.toString(); // TODO: find something better
	}
}
