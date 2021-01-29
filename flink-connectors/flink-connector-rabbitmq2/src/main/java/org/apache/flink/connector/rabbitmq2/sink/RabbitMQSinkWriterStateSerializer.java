package org.apache.flink.connector.rabbitmq2.sink;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.ArrayList;

public class RabbitMQSinkWriterStateSerializer implements SimpleVersionedSerializer<RabbitMQSinkState> {
    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(RabbitMQSinkState rabbitMQSinkState) throws IOException {
        return new byte[0];
    }

    @Override
    public RabbitMQSinkState deserialize(int i, byte[] bytes) throws IOException {
        return new RabbitMQSinkState(new ArrayList<>());
    }
}
