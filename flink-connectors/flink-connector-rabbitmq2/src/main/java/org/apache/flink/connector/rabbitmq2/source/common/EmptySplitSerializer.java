package org.apache.flink.connector.rabbitmq2.source.common;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

public class EmptySplitSerializer implements SimpleVersionedSerializer<EmptyPartitionSplit> {
	@Override
	public int getVersion() {
		return 0;
	}

	@Override
	public byte[] serialize(EmptyPartitionSplit emptyPartitionSplit) {
		// TODO: Check that this is correct
		return new byte[0];
	}

	@Override
	public EmptyPartitionSplit deserialize(int i, byte[] bytes) {
		return new EmptyPartitionSplit();
	}
}
