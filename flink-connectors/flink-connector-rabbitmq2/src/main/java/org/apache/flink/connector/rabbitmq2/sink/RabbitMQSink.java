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

    public static final int defaultMaxRetry = 5;
    public static final ConsistencyMode defaultConsistencyMode = ConsistencyMode.AT_MOST_ONCE;

    public RabbitMQSink(RMQConnectionConfig connectionConfig, String queueName, SerializationSchema<T> serializationSchema, ConsistencyMode consistencyMode, RabbitMQSinkPublishOptions<T> publishOptions, int maxRetry, SerializableReturnListener returnListener) {
        this.connectionConfig = connectionConfig;
        this.queueName = queueName;
        this.serializationSchema = serializationSchema;
        this.publishOptions = publishOptions;
        this.consistencyMode = consistencyMode;
        this.maxRetry = maxRetry;
        this.returnListener = returnListener;

        Preconditions.checkState(verifyPublishOptions(), "");
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
                return new RabbitMQSinkWriterAtMostOnce<>(connectionConfig, queueName, serializationSchema, publishOptions, maxRetry, returnListener, states);
            case AT_LEAST_ONCE:
                return new RabbitMQSinkWriterAtLeastOnce<>(connectionConfig, queueName, serializationSchema, publishOptions, maxRetry, returnListener, states);
            case EXACTLY_ONCE:
                return new RabbitMQSinkWriterExactlyOnce<>(connectionConfig, queueName, serializationSchema, publishOptions, maxRetry, returnListener, states);
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
        return Optional.of(new RabbitMQSinkWriterStateSerializer<>());
    }
}
