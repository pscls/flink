package org.apache.flink.connector.rabbitmq2.source.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;


public class RabbitMQSplitSerializer implements SimpleVersionedSerializer<RabbitMQPartitionSplit> {

	private static final int CURRENT_VERSION = 0;

	@Override
	public int getVersion() {
		return CURRENT_VERSION;
	}

	@Override
	public byte[] serialize(RabbitMQPartitionSplit split) throws IOException {
		try (
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutputStream out = new DataOutputStream(baos)
		) {
//			out.writeUTF(split.getTopic());
//			out.writeInt(split.getPartition());
//			out.writeLong(split.getStartingOffset());
//			out.writeLong(split.getStoppingOffset().orElse(KafkaPartitionSplit.NO_STOPPING_OFFSET));
//			out.flush();
			return baos.toByteArray();
		}
	}

	@Override
	public RabbitMQPartitionSplit deserialize(int version, byte[] serialized) throws IOException {
		try (
			ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
			DataInputStream in = new DataInputStream(bais)
		) {
//			RMQConnectionConfig config =
//			String topic = in.readUTF();
//			int partition = in.readInt();
//			long offset = in.readLong();
//			long stoppingOffset = in.readLong();
			return new RabbitMQPartitionSplit(null);
		}
	}
}
