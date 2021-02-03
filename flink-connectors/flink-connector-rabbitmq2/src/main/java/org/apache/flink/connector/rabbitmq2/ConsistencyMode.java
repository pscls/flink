package org.apache.flink.connector.rabbitmq2;

public enum ConsistencyMode {
	AT_MOST_ONCE,
    AT_LEAST_ONCE,
	EXACTLY_ONCE,
}
