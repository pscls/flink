package org.apache.flink.connector.rabbitmq2.sink.specalized;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.rabbitmq2.sink.RabbitMQSinkState;
import org.apache.flink.connector.rabbitmq2.sink.RabbitMQSinkWriterBase;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.util.List;

public class RabbitMQSinkWriterExactlyOnce<T> extends RabbitMQSinkWriterBase<T> {
    public RabbitMQSinkWriterExactlyOnce(
            RMQConnectionConfig connectionConfig,
            String queueName,
            SerializationSchema<T> serializationSchema,
            List<RabbitMQSinkState> states) {
        super(connectionConfig, queueName, serializationSchema);
    }
}
