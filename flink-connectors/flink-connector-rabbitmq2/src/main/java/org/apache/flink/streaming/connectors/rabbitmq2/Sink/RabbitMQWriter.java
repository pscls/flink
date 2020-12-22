package org.apache.flink.streaming.connectors.rabbitmq2.Sink;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.io.IOException;
import java.util.List;

public class RabbitMQWriter<IN> implements SinkWriter<IN, RabbitMQSinkCommittable, RabbitMQWriterState> {

	public RabbitMQWriter(RMQConnectionConfig config) {

	}

	@Override
	public void write(IN in, Context context) throws IOException {

	}

	@Override
	public List<RabbitMQSinkCommittable> prepareCommit(boolean b) throws IOException {
		return null;
	}

	@Override
	public List<RabbitMQWriterState> snapshotState() throws IOException {
		return null;
	}

	@Override
	public void close() throws Exception {

	}
}
