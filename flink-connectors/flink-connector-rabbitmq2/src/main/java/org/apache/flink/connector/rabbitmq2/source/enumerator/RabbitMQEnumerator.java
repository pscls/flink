package org.apache.flink.connector.rabbitmq2.source.enumerator;

import org.apache.commons.compress.utils.Lists;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.rabbitmq2.source.split.RabbitMQPartitionSplit;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RabbitMQEnumerator implements SplitEnumerator<RabbitMQPartitionSplit, RabbitMQSourceEnumState> {

//	private final RMQConnectionConfig connectionConfig;
	private final SplitEnumeratorContext<RabbitMQPartitionSplit> context;
	private final HashMap<Integer, Set<RabbitMQPartitionSplit>> splitAssignments;
	private final RabbitMQPartitionSplit masterSplit;

	public RabbitMQEnumerator(RMQConnectionConfig connectionConfig,
							  SplitEnumeratorContext<RabbitMQPartitionSplit> context) {
		this(connectionConfig, context, new HashMap<>());
	}
	public RabbitMQEnumerator(RMQConnectionConfig connectionConfig,
							  SplitEnumeratorContext<RabbitMQPartitionSplit> context, Map<Integer, Set<RabbitMQPartitionSplit>> currentSplitsAssignments) {
//		this.connectionConfig = connectionConfig;
		this.context = context;
		this.splitAssignments = new HashMap<>(currentSplitsAssignments);
		this.masterSplit = new RabbitMQPartitionSplit(connectionConfig);
	}

	@Override
	public void start() {
		assignPendingPartitionSplits();
	}

	@Override
	public void handleSplitRequest(int i, @Nullable String s) {
		// the rabbitmq source pushes splits eagerly, rather than act upon split requests
	}

	@Override
	public void addSplitsBack(List<RabbitMQPartitionSplit> list, int i) {
		splitAssignments.remove(i);
		assignPendingPartitionSplits();
	}

	@Override
	public void addReader(int i) {
		assignPendingPartitionSplits();
	}

	@Override
	public RabbitMQSourceEnumState snapshotState() throws Exception {
		return new RabbitMQSourceEnumState(splitAssignments);
	}

	@Override
	public void close() throws IOException {
		// TODO: tell source reader to close their rabbitmq connections
	}

	//	PRIVATE METHODS
	private void assignPendingPartitionSplits() {
		HashMap<Integer, Set<RabbitMQPartitionSplit>> pendingAssignments = new HashMap<>();

		for (int readerId : context.registeredReaders().keySet()) {
			if (!splitAssignments.containsKey(readerId)) { //TODO: split assignment could be a simple hashset of reader ids to make it easier
				pendingAssignments.put(readerId, new HashSet<>(Arrays.asList(masterSplit)));
			}
		}
		context.assignSplits(new SplitsAssignment(pendingAssignments));
		splitAssignments.putAll(pendingAssignments);
	}
}
