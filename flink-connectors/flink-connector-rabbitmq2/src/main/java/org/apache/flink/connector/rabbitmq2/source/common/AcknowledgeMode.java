package org.apache.flink.connector.rabbitmq2.source.common;

public enum AcknowledgeMode {
	AUTO, // rabbitmq auto-acknowledgement mode
	POLLING, // acknowledge when the message gets polled
	CHECKPOINT // acknowledge after checkpoint (use it only when checkpointing is enabled)
}
