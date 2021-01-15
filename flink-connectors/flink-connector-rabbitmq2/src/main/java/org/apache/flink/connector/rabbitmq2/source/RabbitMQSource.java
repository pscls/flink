package org.apache.flink.connector.rabbitmq2.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
//import org.apache.flink.connector.rabbitmq2.source.enumerator.KafkaSourceEnumStateSerializer;
//import org.apache.flink.connector.rabbitmq2.source.enumerator.KafkaSourceEnumerator;
//import org.apache.flink.connector.rabbitmq2.source.enumerator.initializer.OffsetsInitializer;
//import org.apache.flink.connector.rabbitmq2.source.enumerator.subscriber.KafkaSubscriber;
//import org.apache.flink.connector.rabbitmq2.source.reader.KafkaPartitionSplitReader;
//import org.apache.flink.connector.rabbitmq2.source.reader.KafkaRecordEmitter;
//import org.apache.flink.connector.rabbitmq2.source.reader.KafkaSourceReader;
//import org.apache.flink.connector.rabbitmq2.source.reader.deserializer.KafkaRecordDeserializer;
import org.apache.flink.connector.rabbitmq2.source.common.AcknowledgeMode;
import org.apache.flink.connector.rabbitmq2.source.common.ConsistencyMode;
import org.apache.flink.connector.rabbitmq2.source.common.EmptyEnumCheckpointSerializer;
import org.apache.flink.connector.rabbitmq2.source.common.EmptyEnumState;
import org.apache.flink.connector.rabbitmq2.source.common.EmptyEnumerator;
import org.apache.flink.connector.rabbitmq2.source.common.EmptyPartitionSplit;
import org.apache.flink.connector.rabbitmq2.source.common.EmptySplitSerializer;
import org.apache.flink.connector.rabbitmq2.source.reader.RabbitMQSourceReaderBase;
//import org.apache.flink.connector.rabbitmq2.source.split.KafkaPartitionSplitSerializer;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.rabbitmq2.source.reader.specialized.RabbitMQSourceReaderAtLeastOnce;
import org.apache.flink.connector.rabbitmq2.source.reader.specialized.RabbitMQSourceReaderAtLeastOnceAfterCheckpoint;
import org.apache.flink.connector.rabbitmq2.source.reader.specialized.RabbitMQSourceReaderAtMostOnce;
import org.apache.flink.connector.rabbitmq2.source.reader.specialized.RabbitMQSourceReaderExactlyOnce;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RabbitMQSource<OUT> implements Source<OUT, EmptyPartitionSplit, EmptyEnumState>, ResultTypeQueryable<OUT> {
	private static final Logger LOG = LoggerFactory.getLogger(RabbitMQSourceReaderBase.class);

	private final RMQConnectionConfig connectionConfig;
	private final String queueName;
//	private final AcknowledgeMode acknowledgeMode;
	private final ConsistencyMode consistencyMode;
	protected DeserializationSchema<OUT> deliveryDeserializer;

	public RabbitMQSource (RMQConnectionConfig connectionConfig, String queueName, DeserializationSchema<OUT> deserializationSchema, ConsistencyMode consistencyMode) {
		this.connectionConfig = connectionConfig;
		this.queueName = queueName;
		this.deliveryDeserializer = deserializationSchema;
//		this.acknowledgeMode = AcknowledgeMode.POLLING;
		this.consistencyMode = consistencyMode;

		System.out.println("Create SOURCE");
	}

	public RabbitMQSource (RMQConnectionConfig connectionConfig, String queueName, DeserializationSchema<OUT> deserializationSchema) {
		this(connectionConfig, queueName, deserializationSchema, ConsistencyMode.AT_LEAST_ONCE);
	}

	public static <OUT> RabbitMQSourceBuilder<OUT> builder() {
		return new RabbitMQSourceBuilder<>();
	}

	@Override
	public Boundedness getBoundedness() {
		return Boundedness.CONTINUOUS_UNBOUNDED;
	}

	@Override
	public SourceReader<OUT, EmptyPartitionSplit> createReader(SourceReaderContext sourceReaderContext) throws Exception {
		System.out.println("Create READER");
		switch (consistencyMode) {
			case AT_MOST_ONCE:
				return new RabbitMQSourceReaderAtMostOnce<>(connectionConfig, queueName, deliveryDeserializer);
			case AT_LEAST_ONCE_AFTER_CHECKPOINTING:
				return new RabbitMQSourceReaderAtLeastOnceAfterCheckpoint<>(connectionConfig, queueName, deliveryDeserializer);
			case EXACTLY_ONCE:
				return new RabbitMQSourceReaderExactlyOnce<>(connectionConfig, queueName, deliveryDeserializer);
			default:
				// AT_LEAST_ONCE
				return new RabbitMQSourceReaderAtLeastOnce<>(connectionConfig, queueName, deliveryDeserializer);
		}
//		return new RabbitMQSourceReaderBase<>(connectionConfig, queueName, deliveryDeserializer);
	}

	@Override
	public SplitEnumerator<EmptyPartitionSplit, EmptyEnumState> createEnumerator(
		SplitEnumeratorContext<EmptyPartitionSplit> splitEnumeratorContext) throws Exception {
		System.out.println("Create ENUMERATOR");
		return new EmptyEnumerator();
	}

	@Override
	public SplitEnumerator<EmptyPartitionSplit, EmptyEnumState> restoreEnumerator(
		SplitEnumeratorContext<EmptyPartitionSplit> splitEnumeratorContext,
		EmptyEnumState emptyEnumState) throws Exception {
		return new EmptyEnumerator();
	}

	@Override
	public SimpleVersionedSerializer<EmptyPartitionSplit> getSplitSerializer() {
		return new EmptySplitSerializer();
	}

	@Override
	public SimpleVersionedSerializer<EmptyEnumState> getEnumeratorCheckpointSerializer() {
		System.out.println("getEnumeratorCheckpointSerializer");
		return new EmptyEnumCheckpointSerializer();
	}

	@Override
	public TypeInformation<OUT> getProducedType() {
		return deliveryDeserializer.getProducedType();
	}
}
