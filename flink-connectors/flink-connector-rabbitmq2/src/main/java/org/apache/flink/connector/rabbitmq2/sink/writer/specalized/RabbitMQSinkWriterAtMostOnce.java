package org.apache.flink.connector.rabbitmq2.sink.writer.specalized;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.rabbitmq2.sink.RabbitMQSinkPublishOptions;
import org.apache.flink.connector.rabbitmq2.sink.SinkMessage;
import org.apache.flink.connector.rabbitmq2.sink.state.RabbitMQSinkWriterState;
import org.apache.flink.connector.rabbitmq2.sink.writer.RabbitMQSinkWriterBase;
import org.apache.flink.streaming.connectors.rabbitmq.SerializableReturnListener;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.io.IOException;
import java.util.List;

public class RabbitMQSinkWriterAtMostOnce<T> extends RabbitMQSinkWriterBase<T> {
    public RabbitMQSinkWriterAtMostOnce(
            RMQConnectionConfig connectionConfig,
            String queueName,
            SerializationSchema<T> serializationSchema,
            RabbitMQSinkPublishOptions<T> publishOptions,
            SerializableReturnListener returnListener,
            List<RabbitMQSinkWriterState<T>> states) {
        super(connectionConfig, queueName, serializationSchema, publishOptions, 0, returnListener);
    }

    @Override
    public void write(T element, Context context) {
        T timestamp = (T) ((String.valueOf(System.currentTimeMillis())));
        send(timestamp, serializationSchema.serialize(timestamp));
//        send(element, serializationSchema.serialize(element));
    }

}
