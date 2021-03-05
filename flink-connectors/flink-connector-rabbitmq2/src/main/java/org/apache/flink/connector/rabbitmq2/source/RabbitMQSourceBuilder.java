package org.apache.flink.connector.rabbitmq2.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.rabbitmq2.ConsistencyMode;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

/**
 * A @builder class to simplify the creation of a {@link RabbitMQSource}.
 *
 * <p>The following example shows the minimum setup to create a RabbitMQSource that reads String
 * messages from a Queue.
 *
 * <pre>{@code
 * RabbitMQSource<String> source = RabbitMQSource
 *     .<String>builder()
 *     .setConnectionConfig(MY_RMQ_CONNECTION_CONFIG)
 *     .setQueueName("myQueue")
 *     .setDeliveryDeserializer(new SimpleStringSchema())
 *     .setConsistencyMode(MY_CONSISTENCY_MODE)
 *     .build();
 * }</pre>
 *
 * <p>For details about the connection config refer to {@link RMQConnectionConfig}. For details
 * about the available consistency modes refer to {@link ConsistencyMode}.
 *
 * @param <T> the output type of the source.
 */
public class RabbitMQSourceBuilder<T> {
    // The configuration for the rabbitmq connection.
    private RMQConnectionConfig connectionConfig;
    // Name of the queue to consume from.
    private String queueName;
    // The deserializer for the messages of rabbitmq.
    private DeserializationSchema<T> deliveryDeserializer;
    // The consistency mode for the source.
    private ConsistencyMode consistencyMode;

    /**
     * Build the {@link RabbitMQSource}.
     *
     * @return a RabbitMQSource with the configuration set for this builder.
     */
    public RabbitMQSource<T> build() {
        return new RabbitMQSource<>(
                connectionConfig, queueName, deliveryDeserializer, consistencyMode);
    }

    /**
     * Set the connection config for rabbitmq.
     *
     * @param connectionConfig the connection configuration for rabbitmq.
     * @return this RabbitMQSourceBuilder
     * @see RMQConnectionConfig
     */
    public RabbitMQSourceBuilder<T> setConnectionConfig(RMQConnectionConfig connectionConfig) {
        this.connectionConfig = connectionConfig;
        return this;
    }

    /**
     * Set the name of the queue to consume from.
     *
     * @param queueName the name of the queue to consume from.
     * @return this RabbitMQSourceBuilder
     */
    public RabbitMQSourceBuilder<T> setQueueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    /**
     * Set the deserializer for the message deliveries from rabbitmq.
     *
     * @param deliveryDeserializer a deserializer for the message deliveries from rabbitmq.
     * @return this RabbitMQSourceBuilder
     * @see DeserializationSchema
     */
    public RabbitMQSourceBuilder<T> setDeliveryDeserializer(
            DeserializationSchema<T> deliveryDeserializer) {
        this.deliveryDeserializer = deliveryDeserializer;
        return this;
    }

    /**
     * Set the consistency mode for the source.
     *
     * @param consistencyMode the consistency mode for the source.
     * @return this RabbitMQSourceBuilder
     * @see ConsistencyMode
     */
    public RabbitMQSourceBuilder<T> setConsistencyMode(ConsistencyMode consistencyMode) {
        this.consistencyMode = consistencyMode;
        return this;
    }
}
