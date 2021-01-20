package org.apache.flink.connector.rabbitmq2.source.reader.specialized;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.rabbitmq2.source.common.Message;
import org.apache.flink.connector.rabbitmq2.source.reader.RabbitMQSourceReaderBase;
import org.apache.flink.streaming.connectors.rabbitmq.RMQDeserializationSchema;

public class RabbitMQSourceReaderAtLeastOnce<T> extends RabbitMQSourceReaderBase<T> {

	public RabbitMQSourceReaderAtLeastOnce(
		SourceReaderContext sourceReaderContext,
		DeserializationSchema<T> deliveryDeserializer) {
		super(sourceReaderContext, deliveryDeserializer);
	}

	@Override
	protected boolean isAutoAck() {
		return false;
	}

	@Override
	protected void handleMessagePolled(Message<T> message) {
		acknowledgeMessageId(message.getDeliveryTag());
	}
}
