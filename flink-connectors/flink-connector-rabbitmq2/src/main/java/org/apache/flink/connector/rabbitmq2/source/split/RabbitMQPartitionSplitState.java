package org.apache.flink.connector.rabbitmq2.source.split;

import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.util.Set;

public class RabbitMQPartitionSplitState extends RabbitMQPartitionSplit {
	Set<Integer> receivedAndUnacknowledgedMessages;

	public RabbitMQPartitionSplitState(RMQConnectionConfig connectionConfig, Set<Integer> receivedAndUnacknowledgedMessages) {
//		super(connectionConfig);
		this.receivedAndUnacknowledgedMessages = receivedAndUnacknowledgedMessages;
	}
}
