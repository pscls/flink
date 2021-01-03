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
import org.apache.flink.connector.rabbitmq2.source.enumerator.RabbitMQEnumerator;
import org.apache.flink.connector.rabbitmq2.source.enumerator.RabbitMQSourceEnumState;
import org.apache.flink.connector.rabbitmq2.source.reader.RabbitMQSourceReader;
import org.apache.flink.connector.rabbitmq2.source.split.RabbitMQPartitionSplit;
//import org.apache.flink.connector.rabbitmq2.source.split.KafkaPartitionSplitSerializer;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.rabbitmq2.source.split.RabbitMQSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.rabbitmq.RMQDeserializationSchema;
import org.apache.flink.connector.rabbitmq2.source.RabbitMQDeserializationSchemaWrapper;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import org.eclipse.jetty.util.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RabbitMQSource<OUT> implements Source<OUT, RabbitMQPartitionSplit, RabbitMQSourceEnumState>, ResultTypeQueryable<OUT> {
	private static final Logger LOG = LoggerFactory.getLogger(RabbitMQSourceReader.class);

	private final RMQConnectionConfig connectionConfig;
	private final String queueName;
	protected RMQDeserializationSchema<OUT> deliveryDeserializer;

	public RabbitMQSource (RMQConnectionConfig connectionConfig, String queueName, DeserializationSchema<OUT> deserializationSchema) {
		this.connectionConfig = connectionConfig;
		this.queueName = queueName;
		this.deliveryDeserializer = new RabbitMQDeserializationSchemaWrapper<>(deserializationSchema);
		System.out.println("Create SOURCE");
	}

	public static <OUT> RabbitMQSourceBuilder<OUT> builder() {
		return new RabbitMQSourceBuilder<>();
	}

	@Override
	public Boundedness getBoundedness() {
		return Boundedness.CONTINUOUS_UNBOUNDED;
	}

	@Override
	public SourceReader<OUT, RabbitMQPartitionSplit> createReader(SourceReaderContext sourceReaderContext) throws Exception {
//		RabbitMQRecordEmitter<OUT> recordEmitter = new RabbitMQRecordEmitter<>();
		System.out.println("Create READER");
		return new RabbitMQSourceReader<OUT>(connectionConfig, queueName, deliveryDeserializer);
	}

	@Override
	public SplitEnumerator<RabbitMQPartitionSplit, RabbitMQSourceEnumState> createEnumerator(
		SplitEnumeratorContext<RabbitMQPartitionSplit> splitEnumeratorContext) throws Exception {
		System.out.println("Create ENUMERATOR");
		return new RabbitMQEnumerator(connectionConfig, splitEnumeratorContext);
	}

	@Override
	public SplitEnumerator<RabbitMQPartitionSplit, RabbitMQSourceEnumState> restoreEnumerator(
		SplitEnumeratorContext<RabbitMQPartitionSplit> splitEnumeratorContext,
		RabbitMQSourceEnumState rabbitMQSourceEnumState) throws Exception {
		return new RabbitMQEnumerator(
			connectionConfig, splitEnumeratorContext, rabbitMQSourceEnumState.getCurrentAssignment()
		);
	}

	@Override
	public SimpleVersionedSerializer<RabbitMQPartitionSplit> getSplitSerializer() {
		return new RabbitMQSplitSerializer();
	}

	@Override
	public SimpleVersionedSerializer<RabbitMQSourceEnumState> getEnumeratorCheckpointSerializer() {
		System.out.println("getEnumeratorCheckpointSerializer");
		return null;
	}

	@Override
	public TypeInformation<OUT> getProducedType() {
		return deliveryDeserializer.getProducedType();
	}
}
