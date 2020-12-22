package org.apache.flink.streaming.connectors.rabbitmq2.Sink;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * @param <IN> Type of the elements in the input of the sink that are also the elements to be written to its output
 */
public class RabbitMQSink<IN> implements Sink<IN, RabbitMQSinkCommittable, RabbitMQWriterState, Void> {
	RMQConnectionConfig rmqConnectionConfig;
	public RabbitMQSink(RMQConnectionConfig rmqConnectionConfig) {
		this.rmqConnectionConfig = rmqConnectionConfig;
	}

	@Override
	public SinkWriter<IN, RabbitMQSinkCommittable, RabbitMQWriterState> createWriter(
		InitContext initContext,
		List<RabbitMQWriterState> list) throws IOException {
		RabbitMQWriter<IN> writer = new RabbitMQWriter<IN>(rmqConnectionConfig);
		writer.initializeState(states);
		return writer;
	}

	@Override
	public Optional<Committer<RabbitMQSinkCommittable>> createCommitter() throws IOException {
		return Optional.empty();
	}

	@Override
	public Optional<GlobalCommitter<RabbitMQSinkCommittable, Void>> createGlobalCommitter() throws IOException {
		return Optional.empty();
	}

	@Override
	public Optional<SimpleVersionedSerializer<RabbitMQSinkCommittable>> getCommittableSerializer() {
		return Optional.empty();
	}

	@Override
	public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
		return Optional.empty();
	}

	@Override
	public Optional<SimpleVersionedSerializer<RabbitMQWriterState>> getWriterStateSerializer() {
		return Optional.empty();
	}
}
