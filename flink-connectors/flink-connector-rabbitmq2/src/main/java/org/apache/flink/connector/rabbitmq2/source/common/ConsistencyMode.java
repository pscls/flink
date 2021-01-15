package org.apache.flink.connector.rabbitmq2.source.common;

public enum ConsistencyMode {
	AT_MOST_ONCE,
	AT_LEAST_ONCE,
	AT_LEAST_ONCE_AFTER_CHECKPOINTING,
	EXACTLY_ONCE,
}
