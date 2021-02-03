package org.apache.flink.connector.rabbitmq2.sink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSinkPublishOptions;

import java.util.Optional;

public interface RabbitMQSinkPublishOptions<T> extends RMQSinkPublishOptions<T> {

    /**
     * @return
     */
    default Optional<DeserializationSchema<T>> getDeserializationSchema() { return Optional.empty(); }
}
