package org.apache.flink.connector.rabbitmq2.source.reader.specialized;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.rabbitmq2.source.reader.RabbitMQSourceReaderBase;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

public class RabbitMQSourceReaderAtMostOnce<T> extends RabbitMQSourceReaderBase<T> {

	public RabbitMQSourceReaderAtMostOnce(
		SourceReaderContext sourceReaderContext,
		RMQConnectionConfig rmqConnectionConfig,
		DeserializationSchema<T> deliveryDeserializer) {
		super(sourceReaderContext, rmqConnectionConfig, deliveryDeserializer);
	}

	@Override
	protected boolean isAutoAck() {
		return true;
	}
}
