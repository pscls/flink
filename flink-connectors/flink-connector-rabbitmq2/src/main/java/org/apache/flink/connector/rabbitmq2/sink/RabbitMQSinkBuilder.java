package org.apache.flink.connector.rabbitmq2.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.rabbitmq2.ConsistencyMode;
import org.apache.flink.streaming.connectors.rabbitmq.SerializableReturnListener;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

/**
 * A Builder for the {@link RabbitMQSink}. Available consistency modes are contained in
 * {@link ConsistencyMode} Required parameters are a {@code queueName}, a {@code connectionConfig}
 * and a {@code serializationSchema}
 *
 * <pre>
 *  RabbitMQSink
 *    .builder()
 *    .setConnectionConfig(connectionConfig)
 *    .setQueueName("queue")
 *    .setSerializationSchema(new SimpleStringSchema())
 *    .setConsistencyMode(ConsistencyMode.AT_LEAST_ONCE)
 *    .setMinimalResendInterval(10L)
 *    .build();
 * </pre>
 *
 * ======= A Builder for the RabbitMQSink. Available consistency modes are contained in {@link
 * ConsistencyMode} Required parameters are a {@code queueName}, a {@code connectionConfig} and a
 * {@code serializationSchema}
 */
public class RabbitMQSinkBuilder<T> {

    private String queueName;
    private RMQConnectionConfig connectionConfig;
    private SerializationSchema<T> serializationSchema;
    private ConsistencyMode consistencyMode;
    private RabbitMQSinkPublishOptions<T> publishOptions;
    private Integer maxRetry;
    private Long minimalResendIntervalMilliseconds;
    private SerializableReturnListener returnListener;

    public RabbitMQSinkBuilder() {
        this.consistencyMode = RabbitMQSink.DEFAULT_CONSISTENCY_MODE;
        this.maxRetry = RabbitMQSink.DEFAULT_MAX_RETRY;
    }

    public RabbitMQSink<T> build() {
        return new RabbitMQSink<>(
                connectionConfig,
                queueName,
                serializationSchema,
                consistencyMode,
                returnListener,
                publishOptions,
                maxRetry,
                minimalResendIntervalMilliseconds);
    }

    /**
     * Sets the RMQConnectionConfig for this sink.
     *
     * @param connectionConfig configuration required to connect to RabbitMQ
     * @return this builder
     */
    public RabbitMQSinkBuilder<T> setConnectionConfig(RMQConnectionConfig connectionConfig) {
        this.connectionConfig = connectionConfig;
        return this;
    }

    /**
     * Sets the name of the queue to publish to.
     *
     * @param queueName name of an existing queue in RabbitMQ
     * @return this builder
     */
    public RabbitMQSinkBuilder<T> setQueueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    /**
     * Sets the SerializationSchema used to serialize incoming objects.
     *
     * @param serializationSchema the serialization schema to use
     * @return this builder
     */
    public RabbitMQSinkBuilder<T> setSerializationSchema(
            SerializationSchema<T> serializationSchema) {
        this.serializationSchema = serializationSchema;
        return this;
    }

    /**
     * Sets the RabbitMQSinkPublishOptions for this sink. Publish options can be used for routing in
     * an exchange in RabbitMQ.
     *
     * @param publishOptions the publish options to be used
     * @return this builder
     */
    public RabbitMQSinkBuilder<T> setPublishOptions(RabbitMQSinkPublishOptions<T> publishOptions) {
        this.publishOptions = publishOptions;
        return this;
    }

    /**
     * Set the ConsistencyMode for this sink to operate in. Available modes are AT_MOST_ONCE,
     * AT_LEAST_ONCE and EXACTLY_ONCE
     *
     * @param consistencyMode set the consistency mode
     * @return this builder
     */
    public RabbitMQSinkBuilder<T> setConsistencyMode(ConsistencyMode consistencyMode) {
        this.consistencyMode = consistencyMode;
        return this;
    }

    /**
     * Optional and only relevant for at-least-once and exactly-once behaviour.
     *
     * <p>Set the maximum number of retries for this sink in at-least-once and exactly-once modes.
     * If a sent message is not acknowledged after a certain interval, it will be resent on the next
     * checkpoint. After the retry threshold is reached, an exception will be thrown.
     *
     * @param maxRetry sets the maximum number of retries to send each message.
     * @return this builder
     */
    public RabbitMQSinkBuilder<T> setMaxRetry(int maxRetry) {
        this.maxRetry = maxRetry;
        return this;
    }

    /**
     * Only relevant for at-least-once behaviour.
     *
     * <p>Set the minimal time interval in milliseconds after which each message is resent if no
     * acknowledgement arrived from RabbitMQ. Because the sink resends messages on checkpoints, this
     * prevents the sink from resending messages immediately if the checkpoint interval is too small.
     *
     * @param minimalResendIntervalMilliseconds the minimal interval to resend messages in ms
     * @return this builder
     */
    public RabbitMQSinkBuilder<T> setMinimalResendInterval(Long minimalResendIntervalMilliseconds) {
        this.minimalResendIntervalMilliseconds = minimalResendIntervalMilliseconds;
        return this;
    }

    /**
     * Set the SerializableReturnListener for this sink.
     *
     * @param returnListener the return listener to use
     * @return this builder
     */
    public RabbitMQSinkBuilder<T> setReturnListener(SerializableReturnListener returnListener) {
        this.returnListener = returnListener;
        return this;
    }
}
