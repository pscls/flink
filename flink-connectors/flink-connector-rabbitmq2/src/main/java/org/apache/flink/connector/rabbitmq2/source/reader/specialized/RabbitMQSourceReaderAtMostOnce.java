package org.apache.flink.connector.rabbitmq2.source.reader.specialized;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.rabbitmq2.source.reader.RabbitMQSourceReaderBase;

/**
 * The RabbitMQSourceReaderAtMostOnce provides at-most-once guarantee. Messages are automatically
 * acknowledged when received from rabbitmq and afterwards consumed by the output. In case of a
 * failure in Flink messages might be lost.
 *
 * @param <T> The output type of the source.
 * @see RabbitMQSourceReaderBase
 */
public class RabbitMQSourceReaderAtMostOnce<T> extends RabbitMQSourceReaderBase<T> {

    public RabbitMQSourceReaderAtMostOnce(
            SourceReaderContext sourceReaderContext,
            DeserializationSchema<T> deliveryDeserializer) {
        super(sourceReaderContext, deliveryDeserializer);
    }

    @Override
    protected boolean isAutoAck() {
        return true;
    }
}
