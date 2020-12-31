package org.apache.flink.connector.rabbitmq2.source.reader;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.rabbitmq2.source.split.RabbitMQPartitionSplit;
import org.apache.flink.connector.rabbitmq2.source.split.RabbitMQPartitionSplitState;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

public class RabbitMQSourceReader<T> implements SourceReaderBase<T, RabbitMQPartitionSplit> {

	private final RMQConnectionConfig connectionConfig;
	private final Queue<T> queue;
	private final List<Integer> emittedAndUnacknowledgedMessageIds;

	public RabbitMQSourceReader(RMQConnectionConfig connectionConfig) {
		this.connectionConfig = connectionConfig;
		this.queue = new LinkedList<T>();
		this.emittedAndUnacknowledgedMessageIds = new ArrayList<>();
	}

	@Override
	public void start() {
		// start rmq connection
		// subscribe to rmq-queue and push new messages into this.queue
	}

	@Override
	public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
		T element = queue.poll();
		output.collect(element); //TODO: maybe we want to emit a timestamp as well?
		//emittedAndUnacknowledgedMessageIds.add(element.id);
		return queue.size() > 0 ? InputStatus.MORE_AVAILABLE : InputStatus.NOTHING_AVAILABLE;
	}

	@Override
	public List<RabbitMQPartitionSplit> snapshotState(long checkpointId) {
		return null;
	}

	@Override
	public CompletableFuture<Void> isAvailable() {
		return null;
	}

	@Override
	public void addSplits(List<RabbitMQPartitionSplit> splits) {

	}

	@Override
	public void notifyNoMoreSplits() {

	}

	@Override
	public void close() throws Exception {

	}
}
