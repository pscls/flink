package org.apache.flink.connector.rabbitmq2.sink;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

public class RabbitMQSinkCommittableSerializer implements SimpleVersionedSerializer<RabbitMQSinkCommittable> {
    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(RabbitMQSinkCommittable rabbitMQSinkCommittable) throws IOException {
        return new byte[0];
    }

    @Override
    public RabbitMQSinkCommittable deserialize(int i, byte[] bytes) throws IOException {
        return new RabbitMQSinkCommittable();
    }
}
