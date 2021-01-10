package org.apache.flink.connector.rabbitmq2.source.common;

import org.apache.flink.api.connector.source.SourceSplit;

public class EmptyPartitionSplit implements SourceSplit {
	@Override
	public String splitId() {
		// TODO: Check that this is correct
		return "0";
	}
}
