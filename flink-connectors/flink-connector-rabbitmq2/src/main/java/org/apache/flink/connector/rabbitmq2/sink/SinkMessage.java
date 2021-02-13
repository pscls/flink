package org.apache.flink.connector.rabbitmq2.sink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;


public class SinkMessage<T> {
    private T message;
    private byte[] bytes;
    private int retries;

    public SinkMessage(T message, byte[] bytes) {
        this(message, bytes, 0);
    }

    public SinkMessage(byte[] bytes, int retries) {
        this.bytes = bytes;
        this.retries = retries;
    }

    public SinkMessage(T message, byte[] bytes, int retries) {
        this.message = message;
        this.bytes = bytes;
        this.retries = retries;
    }

    public int getRetries() { return retries; }

    public int addRetries() {
        retries += 1;
        return retries;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public T getMessage() {
        return message;
    }
}
