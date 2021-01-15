package org.apache.flink.connector.rabbitmq2.source.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class RabbitMQPartitionSplitSerializer implements SimpleVersionedSerializer<RabbitMQPartitionSplit> {
	private static final int CURRENT_VERSION = 0;

	@Override
	public int getVersion() {
		return CURRENT_VERSION;
	}

	@Override
	public byte[] serialize(RabbitMQPartitionSplit rabbitMQPartitionSplit) throws IOException {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
			 DataOutputStream out = new DataOutputStream(baos);
			 ObjectOutputStream objectOutputStream = new ObjectOutputStream(out)) {
			objectOutputStream.writeObject(rabbitMQPartitionSplit);
			out.flush();
			return baos.toByteArray();
		}
	}

	@Override
	public RabbitMQPartitionSplit deserialize(int i, byte[] bytes) throws IOException {
		try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
			 DataInputStream in = new DataInputStream(bais);
			 ObjectInputStream objectInputStream = new ObjectInputStream(in)) {
			return (RabbitMQPartitionSplit) objectInputStream.readObject();
		} catch (ClassNotFoundException e) {
			throw new IOException(e.getException());
		}
	}
}
