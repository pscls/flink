package org.apache.flink.connector.rabbitmq2.source;

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
import org.apache.flink.connector.rabbitmq2.source.reader.RabbitMQRecordEmitter;
import org.apache.flink.connector.rabbitmq2.source.reader.RabbitMQSourceReader;
import org.apache.flink.connector.rabbitmq2.source.split.RabbitMQPartitionSplit;
//import org.apache.flink.connector.rabbitmq2.source.split.KafkaPartitionSplitSerializer;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

public class RabbitMQSource<OUT> implements Source<OUT, RabbitMQPartitionSplit, RabbitMQSourceEnumState>, ResultTypeQueryable<OUT> {
	private final RMQConnectionConfig connectionConfig;

	RabbitMQSource(RMQConnectionConfig connectionConfig) {
		this.connectionConfig = connectionConfig;
	}

	@Override
	public Boundedness getBoundedness() {
		return null;
	}

	@Override
	public SourceReader<OUT, RabbitMQPartitionSplit> createReader(SourceReaderContext sourceReaderContext) throws Exception {
//		RabbitMQRecordEmitter<OUT> recordEmitter = new RabbitMQRecordEmitter<>();

		return new RabbitMQSourceReader<>(connectionConfig);
	}

	@Override
	public SplitEnumerator<RabbitMQPartitionSplit, RabbitMQSourceEnumState> createEnumerator(
		SplitEnumeratorContext<RabbitMQPartitionSplit> splitEnumeratorContext) throws Exception {
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
		return null;
	}

	@Override
	public SimpleVersionedSerializer<RabbitMQSourceEnumState> getEnumeratorCheckpointSerializer() {
		return null;
	}

	@Override
	public TypeInformation<OUT> getProducedType() {
		return null;
	}
}
