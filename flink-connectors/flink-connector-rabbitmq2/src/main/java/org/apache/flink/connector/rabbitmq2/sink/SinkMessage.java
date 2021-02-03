package org.apache.flink.connector.rabbitmq2.sink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;


public class SinkMessage<T> {
    private transient T message;
    private byte[] bytes;
    private int retries;

    public SinkMessage(T message) {
        this.message = message;
        this.retries = 0;
    }

    public SinkMessage(byte[] bytes) {
        this.bytes = bytes;
        this.retries = 0;
    }

    public int getRetries() { return retries; }

    public int addRetries() {
        retries += 1;
        return retries;
    }

    public byte[] getBytes(SerializationSchema<T> serializationSchema) {
        if (bytes == null) {
            bytes = serializationSchema.serialize(message);
        }
        return bytes;
    }

    public T getMessage(Optional<DeserializationSchema<T>> deserializationSchema) throws IOException {
        if (message == null && deserializationSchema.isPresent()) {
            message = deserializationSchema.get().deserialize(bytes);
        }
        return message;
    }
}
