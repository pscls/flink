package org.apache.flink.connector.rabbitmq2.source.reader.specialized;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.rabbitmq2.source.common.Message;
import org.apache.flink.connector.rabbitmq2.source.reader.RabbitMQSourceReaderBase;
import org.apache.flink.connector.rabbitmq2.source.split.RabbitMQPartitionSplit;
import org.apache.flink.streaming.connectors.rabbitmq.RMQDeserializationSchema;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

public class RabbitMQSourceReaderAtLeastOnce<T> extends RabbitMQSourceReaderBase<T> {
	protected List<Long> polledAndUnacknowledgedMessageIds;
	private final Deque<Tuple2<Long, List<Long>>> polledAndUnacknowledgedMessageIdsPerCheckpoint;

	public RabbitMQSourceReaderAtLeastOnce(
		SourceReaderContext sourceReaderContext,
		DeserializationSchema<T> deliveryDeserializer) {
		super(sourceReaderContext, deliveryDeserializer);
		this.polledAndUnacknowledgedMessageIds = new ArrayList<>();
		this.polledAndUnacknowledgedMessageIdsPerCheckpoint = new ArrayDeque<>();
	}

	@Override
	protected boolean isAutoAck() {
		return false;
	}

	@Override
	protected void handleMessagePolled(Message<T> message) {
		this.polledAndUnacknowledgedMessageIds.add(message.getDeliveryTag());
	}

	@Override
	public List<RabbitMQPartitionSplit> snapshotState(long checkpointId) {
		Tuple2<Long, List<Long>> tuple = new Tuple2<>(checkpointId, polledAndUnacknowledgedMessageIds);
		polledAndUnacknowledgedMessageIdsPerCheckpoint.add(tuple);
		polledAndUnacknowledgedMessageIds = new ArrayList<>();

		return super.snapshotState(checkpointId);
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) {
		Iterator<Tuple2<Long, List<Long>>> checkpointIterator = polledAndUnacknowledgedMessageIdsPerCheckpoint.iterator();
		while (checkpointIterator.hasNext()) {
			final Tuple2<Long, List<Long>> nextCheckpoint = checkpointIterator.next();
			long nextCheckpointId = nextCheckpoint.f0;
			if (nextCheckpointId <= checkpointId) {
				acknowledgeMessageIds(nextCheckpoint.f1);
				checkpointIterator.remove();
			}
		}
	}
}
