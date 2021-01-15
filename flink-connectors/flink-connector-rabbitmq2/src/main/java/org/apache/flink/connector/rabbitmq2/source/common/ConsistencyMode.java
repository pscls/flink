package org.apache.flink.connector.rabbitmq2.source.common;

import org.apache.flink.connector.rabbitmq2.source.reader.RabbitMQSourceReaderBase;
import org.apache.flink.connector.rabbitmq2.source.reader.specialized.RabbitMQSourceReaderAtMostOnce;

public enum ConsistencyMode {
	AT_MOST_ONCE,
	AT_LEAST_ONCE,
	AT_LEAST_ONCE_AFTER_CHECKPOINTING,
	EXACTLY_ONCE,
}
