package org.apache.flink.connector.rabbitmq2.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.rabbitmq2.ConsistencyMode;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

public class RabbitMQSourceBuilder<T> {
    private RMQConnectionConfig connectionConfig;
    private String queueName;
    private ConsistencyMode consistencyMode;
    protected DeserializationSchema<T> deliveryDeserializer;

    public RabbitMQSource<T> build() {
        return new RabbitMQSource<>(connectionConfig, queueName, deliveryDeserializer, consistencyMode);
    }

    public RabbitMQSourceBuilder<T> setConnectionConfig(RMQConnectionConfig connectionConfig) {
        this.connectionConfig = connectionConfig;
        return this;
    }

    public RabbitMQSourceBuilder<T> setDeliveryDeserializer(DeserializationSchema<T> deliveryDeserializer) {
        this.deliveryDeserializer = deliveryDeserializer;
        return this;
    }

    public RabbitMQSourceBuilder<T> setConsistencyMode(ConsistencyMode consistencyMode) {
        this.consistencyMode = consistencyMode;
        return this;
    }

    public RabbitMQSourceBuilder<T> setQueueName(String queueName) {
        this.queueName = queueName;
        return this;
    }
}
