package org.apache.flink.connector.rabbitmq2.sink.writer.specalized;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.rabbitmq2.sink.RabbitMQSinkPublishOptions;
import org.apache.flink.connector.rabbitmq2.sink.state.RabbitMQSinkWriterState;
import org.apache.flink.connector.rabbitmq2.sink.writer.RabbitMQSinkWriterBase;
import org.apache.flink.streaming.connectors.rabbitmq.SerializableReturnListener;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.util.List;

public class RabbitMQSinkWriterAtMostOnce<T> extends RabbitMQSinkWriterBase<T> {
    public RabbitMQSinkWriterAtMostOnce(
            RMQConnectionConfig connectionConfig,
            String queueName,
            SerializationSchema<T> serializationSchema,
            RabbitMQSinkPublishOptions<T> publishOptions,
            int maxRetry,
            SerializableReturnListener returnListener,
            List<RabbitMQSinkWriterState<T>> states) {
        super(connectionConfig, queueName, serializationSchema, publishOptions, maxRetry, returnListener);
    }

}
