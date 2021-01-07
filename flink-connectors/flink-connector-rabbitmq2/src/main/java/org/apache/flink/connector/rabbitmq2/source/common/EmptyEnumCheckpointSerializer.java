package org.apache.flink.connector.rabbitmq2.source.common;

import org.apache.flink.core.io.SimpleVersionedSerializer;

public class EmptyEnumCheckpointSerializer implements SimpleVersionedSerializer<EmptyEnumState> {

	@Override
	public int getVersion() {
		return 0;
	}

	@Override
	public byte[] serialize(EmptyEnumState emptyEnumState) {
		return new byte[0];
	}

	@Override
	public EmptyEnumState deserialize(int i, byte[] bytes) {
		return new EmptyEnumState();
	}
}
