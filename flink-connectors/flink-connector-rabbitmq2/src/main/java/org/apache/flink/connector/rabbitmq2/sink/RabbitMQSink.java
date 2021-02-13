package org.apache.flink.connector.rabbitmq2.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.rabbitmq2.sink.state.RabbitMQSinkWriterState;
import org.apache.flink.connector.rabbitmq2.sink.state.RabbitMQSinkWriterStateSerializer;
import org.apache.flink.connector.rabbitmq2.sink.writer.specalized.RabbitMQSinkWriterAtLeastOnce;
import org.apache.flink.connector.rabbitmq2.sink.writer.specalized.RabbitMQSinkWriterAtMostOnce;
import org.apache.flink.connector.rabbitmq2.sink.writer.specalized.RabbitMQSinkWriterExactlyOnce;
import org.apache.flink.connector.rabbitmq2.ConsistencyMode;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.flink.streaming.connectors.rabbitmq.SerializableReturnListener;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

public class RabbitMQSink<T> implements Sink<T, Void, RabbitMQSinkWriterState<T>, Void> {

    private final RMQConnectionConfig connectionConfig;
    private final String queueName;
    private final SerializationSchema<T> serializationSchema;
    private final RabbitMQSinkPublishOptions<T> publishOptions;
    private final ConsistencyMode consistencyMode;
    private final int maxRetry;
    private final SerializableReturnListener returnListener;
    private final Long minimalResendIntervalMilliseconds;

    public static final int defaultMaxRetry = 5;
    public static final ConsistencyMode defaultConsistencyMode = ConsistencyMode.AT_MOST_ONCE;

    public RabbitMQSink(
            RMQConnectionConfig connectionConfig,
            String queueName,
            SerializationSchema<T> serializationSchema,
            ConsistencyMode consistencyMode,
            SerializableReturnListener returnListener,
            @Nullable RabbitMQSinkPublishOptions<T> publishOptions,
            @Nullable Integer maxRetry,
            @Nullable Long minimalResendIntervalMilliseconds) {
        this.connectionConfig = connectionConfig;
        this.queueName = queueName;
        this.serializationSchema = serializationSchema;
        this.consistencyMode = consistencyMode;
        this.returnListener = returnListener;
        this.publishOptions = publishOptions;
        this.maxRetry = maxRetry != null ? maxRetry : defaultMaxRetry;
        this.minimalResendIntervalMilliseconds = minimalResendIntervalMilliseconds;

        Preconditions.checkState(
                verifyPublishOptions(),
                "If consistency mode is stronger than at-most-once and publish options are defined"
                        + "then publish options need a deserialization schema"
        );
    }

    private boolean verifyPublishOptions() {
        // If at_most_once, we don't care if publish options are
        if (consistencyMode == ConsistencyMode.AT_MOST_ONCE) {
            return true;
        }

        if (publishOptions == null) {
            return true;
        }

        return publishOptions.getDeserializationSchema().isPresent();
    }

    public static <T> RabbitMQSinkBuilder<T> builder() {
        return new RabbitMQSinkBuilder<>();
    }

    @Override
    public SinkWriter<T, Void, RabbitMQSinkWriterState<T>> createWriter(
            InitContext context,
            List<RabbitMQSinkWriterState<T>> states) {
        switch (consistencyMode) {
            case AT_MOST_ONCE:
                return new RabbitMQSinkWriterAtMostOnce<>(
                        connectionConfig,
                        queueName,
                        serializationSchema,
                        publishOptions,
                        returnListener,
                        states
                );
            case AT_LEAST_ONCE:
                return new RabbitMQSinkWriterAtLeastOnce<>(
                        connectionConfig,
                        queueName,
                        serializationSchema,
                        publishOptions,
                        maxRetry,
                        returnListener,
                        minimalResendIntervalMilliseconds,
                        states
                );
            case EXACTLY_ONCE:
                return new RabbitMQSinkWriterExactlyOnce<>(
                        connectionConfig,
                        queueName,
                        serializationSchema,
                        publishOptions,
                        maxRetry,
                        returnListener,
                        states
                );
        }
        return null;
    }

    @Override
    public Optional<Committer<Void>> createCommitter() {
//        System.out.println("Create Committer");
        return Optional.empty();
    }

    @Override
    public Optional<GlobalCommitter<Void, Void>> createGlobalCommitter() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<RabbitMQSinkWriterState<T>>> getWriterStateSerializer() {
//        System.out.println("Create Writer Serializer");
        if (publishOptions != null && publishOptions.getDeserializationSchema().isPresent()) {
            return Optional.of(new RabbitMQSinkWriterStateSerializer<>(publishOptions.getDeserializationSchema().get()));
        } else {
            return Optional.of(new RabbitMQSinkWriterStateSerializer<>());
        }
    }
}
