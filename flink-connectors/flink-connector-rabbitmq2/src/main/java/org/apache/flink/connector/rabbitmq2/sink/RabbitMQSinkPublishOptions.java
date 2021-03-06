package org.apache.flink.connector.rabbitmq2.sink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSinkPublishOptions;

import java.util.Optional;

/**
 * Extends sink publish options by providing a deserialization schema for deserializing the
 * serialized messages sent to RabbitMQ by the SinkWriter. This is necessary if at-least or
 * exactly-once is required: On initialization of a SinkWriter after a failure, buffered messages
 * need to be resend. To recompute exchange/routing, the message in its deserialized form is
 * required.
 *
 * @see RMQSinkPublishOptions
 */
public interface RabbitMQSinkPublishOptions<T> extends RMQSinkPublishOptions<T> {

    default Optional<DeserializationSchema<T>> getDeserializationSchema() {
        return Optional.empty();
    }
}
