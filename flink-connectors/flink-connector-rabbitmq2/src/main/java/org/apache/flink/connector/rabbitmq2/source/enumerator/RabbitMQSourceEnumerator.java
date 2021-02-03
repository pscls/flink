package org.apache.flink.connector.rabbitmq2.source.enumerator;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.rabbitmq2.ConsistencyMode;
import org.apache.flink.connector.rabbitmq2.source.split.RabbitMQPartitionSplit;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.util.List;

public class RabbitMQSourceEnumerator implements SplitEnumerator<RabbitMQPartitionSplit, RabbitMQSourceEnumState> {
	private final SplitEnumeratorContext<RabbitMQPartitionSplit> context;
	private final ConsistencyMode consistencyMode;
	private RabbitMQPartitionSplit split;

	public RabbitMQSourceEnumerator(
		SplitEnumeratorContext<RabbitMQPartitionSplit> context,
		ConsistencyMode consistencyMode,
		RMQConnectionConfig connectionConfig,
		String rmqQueueName,
		RabbitMQSourceEnumState enumState // this is not used since the enumerator has no state in this architecture
		) {
		this(context, consistencyMode, connectionConfig, rmqQueueName);
	}

	public RabbitMQSourceEnumerator(
		SplitEnumeratorContext<RabbitMQPartitionSplit> context,
		ConsistencyMode consistencyMode,
		RMQConnectionConfig connectionConfig,
		String rmqQueueName
		) {
		this.context = context;
		this.consistencyMode = consistencyMode;
		this.split = new RabbitMQPartitionSplit(connectionConfig, rmqQueueName);
	}

	@Override
	public void start() {
		System.out.println("Start ENUMERATOR");
	}

	@Override
	public void handleSplitRequest(int i, @Nullable String s) {
		assignSplitToReader(i, split);
	}

	@Override
	public void addSplitsBack(List<RabbitMQPartitionSplit> list, int i) {
		assert list.size() == 1;
		split = list.get(0);
	}

	@Override
	public void addReader(int i) {
		if (consistencyMode == ConsistencyMode.EXACTLY_ONCE && context.currentParallelism() > 1) {
			throw new FlinkRuntimeException("The consistency mode is exactly-once and more than one source reader was created. "
				+ "For exactly once a parallelism higher than one is forbidden.");
		}
	}

	@Override
	public RabbitMQSourceEnumState snapshotState() {
		return new RabbitMQSourceEnumState();
	}

	@Override
	public void close() {
	}

	private void assignSplitToReader(int readerId, RabbitMQPartitionSplit split) {
		SplitsAssignment<RabbitMQPartitionSplit> assignment = new SplitsAssignment<>(split, readerId);
		context.assignSplits(assignment);
	}
}
