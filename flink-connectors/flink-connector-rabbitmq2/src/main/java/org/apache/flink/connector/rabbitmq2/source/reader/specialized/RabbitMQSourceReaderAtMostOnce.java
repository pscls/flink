package org.apache.flink.connector.rabbitmq2.source.reader.specialized;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.rabbitmq2.source.reader.RabbitMQSourceReaderBase;
import org.apache.flink.streaming.connectors.rabbitmq.RMQDeserializationSchema;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

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
