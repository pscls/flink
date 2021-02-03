package org.apache.flink.connector.rabbitmq2.sink.state;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class RabbitMQSinkWriterStateSerializer<T> implements SimpleVersionedSerializer<RabbitMQSinkWriterState<T>> {
    // TODO: Figure out a way to serialize

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(RabbitMQSinkWriterState<T> rabbitMQSinkWriterState) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(out)) {
            objectOutputStream.writeObject(rabbitMQSinkWriterState);
            out.flush();
            return baos.toByteArray();
        }
}

    @Override
    public RabbitMQSinkWriterState<T> deserialize(int i, byte[] bytes) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             DataInputStream in = new DataInputStream(bais);
             ObjectInputStream objectInputStream = new ObjectInputStream(in)) {
            return (RabbitMQSinkWriterState<T>) objectInputStream.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(e.getException());
        }
    }
}
