package org.apache.flink.connector.rabbitmq2.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.rabbitmq2.source.reader.RabbitMQSourceReaderBase;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class RabbitMQSink<T> implements Sink<T, RabbitMQSinkCommitable, RabbitMQSinkBucketState, Void> {

    private final RMQConnectionConfig connectionConfig;
    private final String queueName;
    private final SerializationSchema<T> serializationSchema;

    public RabbitMQSink(RMQConnectionConfig connectionConfig, String queueName, SerializationSchema<T> serializationSchema) {
        this.connectionConfig = connectionConfig;
        this.queueName = queueName;
        this.serializationSchema = serializationSchema;
    }

    @Override
    public SinkWriter<T, RabbitMQSinkCommitable, RabbitMQSinkBucketState> createWriter(
            InitContext context,
            List<RabbitMQSinkBucketState> states) throws IOException {
        return new RabbitMQSinkWriter<>(connectionConfig, queueName, serializationSchema);
    }

    @Override
    public Optional<Committer<RabbitMQSinkCommitable>> createCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalCommitter<RabbitMQSinkCommitable, Void>> createGlobalCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<RabbitMQSinkCommitable>> getCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<RabbitMQSinkBucketState>> getWriterStateSerializer() {
        return Optional.empty();
    }
}
