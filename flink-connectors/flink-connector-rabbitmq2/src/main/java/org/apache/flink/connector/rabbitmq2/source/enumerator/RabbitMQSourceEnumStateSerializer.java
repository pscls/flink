package org.apache.flink.connector.rabbitmq2.source.enumerator;

import org.apache.flink.core.io.SimpleVersionedSerializer;

public class RabbitMQSourceEnumStateSerializer implements SimpleVersionedSerializer<RabbitMQSourceEnumState> {
	@Override
	public int getVersion() {
		return 0;
	}

	@Override
	public byte[] serialize(RabbitMQSourceEnumState rabbitMQSourceEnumState) {
		return new byte[0];
	}

	@Override
	public RabbitMQSourceEnumState deserialize(int i, byte[] bytes) {
		return new RabbitMQSourceEnumState();
	}
}
