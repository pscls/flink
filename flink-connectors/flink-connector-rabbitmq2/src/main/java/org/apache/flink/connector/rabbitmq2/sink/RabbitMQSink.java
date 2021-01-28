package org.apache.flink.connector.rabbitmq2.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.rabbitmq2.sink.specalized.RabbitMQSinkWriterAtLeastOnce;
import org.apache.flink.connector.rabbitmq2.sink.specalized.RabbitMQSinkWriterAtMostOnce;
import org.apache.flink.connector.rabbitmq2.sink.specalized.RabbitMQSinkWriterExactlyOnce;
import org.apache.flink.connector.rabbitmq2.source.common.ConsistencyMode;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class RabbitMQSink<T> implements Sink<T, RabbitMQSinkCommittable, RabbitMQSinkState, Void> {

    private final RMQConnectionConfig connectionConfig;
    private final String queueName;
    private final SerializationSchema<T> serializationSchema;
    private final ConsistencyMode consistencyMode;

    public RabbitMQSink(RMQConnectionConfig connectionConfig, String queueName, SerializationSchema<T> serializationSchema, ConsistencyMode consistencyMode) {
        this.connectionConfig = connectionConfig;
        this.queueName = queueName;
        this.serializationSchema = serializationSchema;
        this.consistencyMode = consistencyMode;
    }

    @Override
    public SinkWriter<T, RabbitMQSinkCommittable, RabbitMQSinkState> createWriter(
            InitContext context,
            List<RabbitMQSinkState> states) throws IOException {
        switch (consistencyMode) {
            case AT_MOST_ONCE:
                return new RabbitMQSinkWriterAtMostOnce<>(connectionConfig, queueName, serializationSchema, states);
            case AT_LEAST_ONCE:
                return new RabbitMQSinkWriterAtLeastOnce<>(connectionConfig, queueName, serializationSchema, states);
            case EXACTLY_ONCE:
                return new RabbitMQSinkWriterExactlyOnce<>(connectionConfig, queueName, serializationSchema, states);
        }
        return null;
    }

    @Override
    public Optional<Committer<RabbitMQSinkCommittable>> createCommitter() throws IOException {
        System.out.println("Create Commiter");
        return Optional.of(new RabbitMQSinkCommitter());
    }

    @Override
    public Optional<GlobalCommitter<RabbitMQSinkCommittable, Void>> createGlobalCommitter() throws IOException {
        System.out.println("Create GLOBAL Commiter");
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<RabbitMQSinkCommittable>> getCommittableSerializer() {
        System.out.println("Create Commiter Serializer");
        return Optional.of(new RabbitMQSinkCommittableSerializer());
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        System.out.println("Create GLOBAL Commiter Serializer");
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<RabbitMQSinkState>> getWriterStateSerializer() {
        System.out.println("Create Writer Serializer");
        return Optional.empty();
    }
}
