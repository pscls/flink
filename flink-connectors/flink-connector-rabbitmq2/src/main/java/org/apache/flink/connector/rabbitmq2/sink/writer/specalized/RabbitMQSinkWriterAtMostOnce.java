package org.apache.flink.connector.rabbitmq2.sink.writer.specalized;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.rabbitmq2.sink.RabbitMQSink;
import org.apache.flink.connector.rabbitmq2.sink.RabbitMQSinkPublishOptions;
import org.apache.flink.connector.rabbitmq2.sink.state.RabbitMQSinkWriterState;
import org.apache.flink.connector.rabbitmq2.sink.writer.RabbitMQSinkWriterBase;
import org.apache.flink.streaming.connectors.rabbitmq.SerializableReturnListener;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.util.List;

/**
 * A {@link SinkWriter} implementation for {@link RabbitMQSink}.
 *
 * It uses exclusively the basic functionalities provided in {@link RabbitMQSinkWriterBase} for
 * publishing messages to RabbitMQ (serializing a stream element and publishing it to RabbitMQ
 * in a fire-and-forget fashion).
 **/
public class RabbitMQSinkWriterAtMostOnce<T> extends RabbitMQSinkWriterBase<T> {
    public RabbitMQSinkWriterAtMostOnce(
            RMQConnectionConfig connectionConfig,
            String queueName,
            SerializationSchema<T> serializationSchema,
            RabbitMQSinkPublishOptions<T> publishOptions,
            SerializableReturnListener returnListener) {
        super(connectionConfig, queueName, serializationSchema, publishOptions, 0, returnListener);
    }

    @Override
    public void write(T element, Context context) {
        send(element, serializationSchema.serialize(element));
    }
}
