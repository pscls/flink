package org.apache.flink.connector.rabbitmq2.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.rabbitmq2.source.common.ConsistencyMode;
import org.apache.flink.connector.rabbitmq2.source.enumerator.RabbitMQSourceEnumState;
import org.apache.flink.connector.rabbitmq2.source.enumerator.RabbitMQSourceEnumStateSerializer;
import org.apache.flink.connector.rabbitmq2.source.enumerator.RabbitMQSourceEnumerator;
import org.apache.flink.connector.rabbitmq2.source.reader.RabbitMQSourceReaderBase;
import org.apache.flink.connector.rabbitmq2.source.reader.specialized.RabbitMQSourceReaderAtLeastOnce;
import org.apache.flink.connector.rabbitmq2.source.reader.specialized.RabbitMQSourceReaderAtLeastOnceAfterCheckpoint;
import org.apache.flink.connector.rabbitmq2.source.reader.specialized.RabbitMQSourceReaderAtMostOnce;
import org.apache.flink.connector.rabbitmq2.source.reader.specialized.RabbitMQSourceReaderExactlyOnce;
import org.apache.flink.connector.rabbitmq2.source.split.RabbitMQPartitionSplit;
import org.apache.flink.connector.rabbitmq2.source.split.RabbitMQPartitionSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RabbitMQSource<OUT> implements Source<OUT, RabbitMQPartitionSplit, RabbitMQSourceEnumState>, ResultTypeQueryable<OUT> {
	private static final Logger LOG = LoggerFactory.getLogger(RabbitMQSourceReaderBase.class);

	private final RMQConnectionConfig connectionConfig;
	private final String queueName;
	private final ConsistencyMode consistencyMode;
	protected DeserializationSchema<OUT> deliveryDeserializer;

	public RabbitMQSource (RMQConnectionConfig connectionConfig, String queueName, DeserializationSchema<OUT> deserializationSchema, ConsistencyMode consistencyMode) {
		this.connectionConfig = connectionConfig;
		this.queueName = queueName;
		this.deliveryDeserializer = deserializationSchema;
		this.consistencyMode = consistencyMode;

		System.out.println("Create SOURCE");
	}

	public RabbitMQSource (RMQConnectionConfig connectionConfig, String queueName, DeserializationSchema<OUT> deserializationSchema) {
		this(connectionConfig, queueName, deserializationSchema, ConsistencyMode.AT_LEAST_ONCE);
	}

	@Override
	public Boundedness getBoundedness() {
		return Boundedness.CONTINUOUS_UNBOUNDED;
	}

	@Override
	public SourceReader<OUT, RabbitMQPartitionSplit> createReader(SourceReaderContext sourceReaderContext) {
		System.out.println("Create READER");
		switch (consistencyMode) {
			case AT_MOST_ONCE:
				return new RabbitMQSourceReaderAtMostOnce<>(sourceReaderContext, deliveryDeserializer);
			case AT_LEAST_ONCE:
				return new RabbitMQSourceReaderAtLeastOnce<>(sourceReaderContext, deliveryDeserializer);
			case AT_LEAST_ONCE_AFTER_CHECKPOINTING:
				return new RabbitMQSourceReaderAtLeastOnceAfterCheckpoint<>(sourceReaderContext, deliveryDeserializer);
			case EXACTLY_ONCE:
				return new RabbitMQSourceReaderExactlyOnce<>(sourceReaderContext, deliveryDeserializer);
			default:
				return null;
		}
	}

	@Override
	public SplitEnumerator<RabbitMQPartitionSplit, RabbitMQSourceEnumState> createEnumerator(
		SplitEnumeratorContext<RabbitMQPartitionSplit> splitEnumeratorContext) {
		System.out.println("Create ENUMERATOR");
		return new RabbitMQSourceEnumerator(splitEnumeratorContext, consistencyMode, connectionConfig, queueName);
	}

	@Override
	public SplitEnumerator<RabbitMQPartitionSplit, RabbitMQSourceEnumState> restoreEnumerator(
		SplitEnumeratorContext<RabbitMQPartitionSplit> splitEnumeratorContext,
		RabbitMQSourceEnumState enumState) {
		return new RabbitMQSourceEnumerator(splitEnumeratorContext, consistencyMode, connectionConfig, queueName, enumState);
	}

	@Override
	public SimpleVersionedSerializer<RabbitMQPartitionSplit> getSplitSerializer() {
		return new RabbitMQPartitionSplitSerializer();
	}

	@Override
	public SimpleVersionedSerializer<RabbitMQSourceEnumState> getEnumeratorCheckpointSerializer() {
		System.out.println("getEnumeratorCheckpointSerializer");
		return new RabbitMQSourceEnumStateSerializer();
	}

	@Override
	public TypeInformation<OUT> getProducedType() {
		return deliveryDeserializer.getProducedType();
	}
}
