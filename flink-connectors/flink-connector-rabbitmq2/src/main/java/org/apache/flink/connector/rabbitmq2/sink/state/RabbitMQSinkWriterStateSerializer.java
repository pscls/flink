package org.apache.flink.connector.rabbitmq2.sink.state;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.rabbitmq2.sink.SinkMessage;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RabbitMQSinkWriterStateSerializer<T> implements SimpleVersionedSerializer<RabbitMQSinkWriterState<T>> {
    private final DeserializationSchema<T> deserializationSchema;

    public RabbitMQSinkWriterStateSerializer(@Nullable DeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }
    public RabbitMQSinkWriterStateSerializer() {
        this(null);
    }

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(RabbitMQSinkWriterState<T> rabbitMQSinkWriterState) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        writeSinkMessages(out, rabbitMQSinkWriterState.getOutstandingMessages());
        out.flush();
        return baos.toByteArray();
    }

    private void writeSinkMessages(DataOutputStream out, List<SinkMessage<T>> messages) throws IOException {
        out.writeInt(messages.size());
        for (SinkMessage<T> message : messages) {
            out.writeInt(message.getBytes().length);
            out.write(message.getBytes());
            out.writeInt(message.getRetries());
        }
    }

    private List<SinkMessage<T>> readSinkMessages(DataInputStream in) throws IOException {
        final int numberOfMessages = in.readInt();
        List<SinkMessage<T>> messages = new ArrayList<>();
        for (int i = 0; i < numberOfMessages; i++) {
            byte[] bytes = in.readNBytes(in.readInt());
            int retries = in.readInt();
            if (deserializationSchema != null) {
                messages.add(new SinkMessage<>(deserializationSchema.deserialize(bytes), bytes, retries));
            } else {
                messages.add(new SinkMessage<>(bytes, retries));
            }
        }
        return messages;
    }

    @Override
    public RabbitMQSinkWriterState<T> deserialize(int i, byte[] bytes) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(bais);
        return new RabbitMQSinkWriterState<>(readSinkMessages(in));
    }
}
