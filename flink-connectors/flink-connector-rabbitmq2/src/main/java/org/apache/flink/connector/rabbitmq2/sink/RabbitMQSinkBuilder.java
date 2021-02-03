package org.apache.flink.connector.rabbitmq2.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.rabbitmq2.ConsistencyMode;
import org.apache.flink.streaming.connectors.rabbitmq.SerializableReturnListener;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

public class RabbitMQSinkBuilder<T> {

    private String queueName;
    private RMQConnectionConfig connectionConfig;
    private SerializationSchema<T> serializationSchema;
    private ConsistencyMode consistencyMode;
    private RabbitMQSinkPublishOptions<T> publishOptions;
    private int maxRetry;
    private SerializableReturnListener returnListener;

    public RabbitMQSinkBuilder() {
        this.consistencyMode = RabbitMQSink.defaultConsistencyMode;
        this.maxRetry = RabbitMQSink.defaultMaxRetry;
    }

    public RabbitMQSink<T> build() {
        return new RabbitMQSink<>(connectionConfig, queueName, serializationSchema, consistencyMode, publishOptions, maxRetry, returnListener);
    }

    public RabbitMQSinkBuilder<T> setConnectionConfig(RMQConnectionConfig connectionConfig) {
        this.connectionConfig = connectionConfig;
        return this;
    }

    public RabbitMQSinkBuilder<T> setQueueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    public RabbitMQSinkBuilder<T> setSerializationSchema(SerializationSchema<T> serializationSchema) {
        this.serializationSchema = serializationSchema;
        return this;
    }

    public RabbitMQSinkBuilder<T> setPublishOptions(RabbitMQSinkPublishOptions<T> publishOptions) {
        this.publishOptions = publishOptions;
        return this;
    }

    public RabbitMQSinkBuilder<T> setConsistencyMode(ConsistencyMode consistencyMode) {
        this.consistencyMode = consistencyMode;
        return this;
    }

    public RabbitMQSinkBuilder<T> setMaxRetry(int maxRetry) {
        this.maxRetry = maxRetry;
        return this;
    }

    public RabbitMQSinkBuilder<T> setReturnListener(SerializableReturnListener returnListener) {
        this.returnListener = returnListener;
        return this;
    }
}
