package org.apache.flink.connector.rabbitmq2.source.reader;

import akka.remote.Ack;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.rabbitmq2.source.RabbitMQSource;
import org.apache.flink.connector.rabbitmq2.source.common.AcknowledgeMode;
import org.apache.flink.connector.rabbitmq2.source.common.EmptyPartitionSplit;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.rabbitmq.RMQDeserializationSchema;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import org.eclipse.jetty.util.ArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RabbitMQSourceReader<T> implements SourceReader<T, EmptyPartitionSplit> {
	private static final Logger LOG = LoggerFactory.getLogger(RabbitMQSourceReader.class);

	private final RMQConnectionConfig rmqConnectionConfig;
	private final String rmqQueueName;
	private final RabbitMQCollector collector;
	private Connection rmqConnection;
	private Channel rmqChannel;
	private final AcknowledgeMode acknowledgeMode;
	private List<Long> polledAndUnacknowledgedMessageIds;
	private Deque<Tuple2<Long, List<Long>>> polledAndUnacknowledgedMessageIdsPerCheckpoint;
	private Set<String> polledAndUnacknowledgedCorrelationIds;

	protected transient boolean autoAck = false;
	private final boolean usesCorrelationId = false;
	protected transient List<Long> sessionIds = new ArrayList<>(64);

	public RabbitMQSourceReader(
		RMQConnectionConfig rmqConnectionConfig,
		String rmqQueueName,
		DeserializationSchema<T> deliveryDeserializer,
		AcknowledgeMode acknowledgeMode) {
		this.rmqConnectionConfig = rmqConnectionConfig;
		this.rmqQueueName = rmqQueueName;
//		this.deliveryDeserializer = deliveryDeserializer;
		this.collector = new RabbitMQCollector<>(deliveryDeserializer, usesCorrelationId);
		this.acknowledgeMode = acknowledgeMode;
		this.polledAndUnacknowledgedMessageIds = new ArrayList<>();
		this.polledAndUnacknowledgedMessageIdsPerCheckpoint = new ArrayDeque<>();
		this.polledAndUnacknowledgedCorrelationIds = new HashSet<>();
	}

	@Override
	public void start() {
		System.out.println("Starting Source Reader");
		setupRabbitMQ();
	}

	private void setupRabbitMQ () {
		try {
			rmqConnection = setupConnection();
			rmqChannel = setupChannel(rmqConnection);
			LOG.info("RabbitMQ Connection was successful: Waiting for messages from the queue. To exit press CTRL+C");
		} catch (IOException | TimeoutException e) {
			LOG.error(e.getMessage());
		}
	}

	private Connection setupConnection() throws IOException, TimeoutException{
		final ConnectionFactory connectionFactory = new ConnectionFactory();
		connectionFactory.setHost(rmqConnectionConfig.getHost());

		return connectionFactory.newConnection();
	}

	private Channel setupChannel(Connection rmqConnection) throws IOException {
		final Channel rmqChannel = rmqConnection.createChannel();
		rmqChannel.queueDeclare(rmqQueueName, true, false, false, null);

		// Set maximum of unacknowledged messages
		if (rmqConnectionConfig.getPrefetchCount().isPresent()) {
			// global: false - the prefetch count is set per consumer, not per rabbitmq channel
			rmqChannel.basicQos(rmqConnectionConfig.getPrefetchCount().get(), false);
		}

		if (acknowledgeMode == AcknowledgeMode.CHECKPOINT) {
			// enable channel commit mode if acknowledging happens after checkpoint
			rmqChannel.txSelect();
		}

		// if masterSplit.hasIds()
		// ack all ids from masterSplit

		final DeliverCallback deliverCallback = (consumerTag, delivery) -> {
			boolean addedToQueue = collector.processMessage(delivery, polledAndUnacknowledgedCorrelationIds);
			if (!addedToQueue) polledAndUnacknowledgedMessageIds.add(delivery.getEnvelope().getDeliveryTag());
		};
		rmqChannel.basicConsume(rmqQueueName, acknowledgeMode == AcknowledgeMode.AUTO, deliverCallback, consumerTag -> {});
		return rmqChannel;
	}

	@Override
	public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
		Tuple2<Long, T> message = collector.getMessage();

		if (message == null) {
			return InputStatus.NOTHING_AVAILABLE;
		}

		output.collect(message.f1); //TODO: maybe we want to emit a timestamp as well?

		if (acknowledgeMode == AcknowledgeMode.POLLING) {
			rmqChannel.basicAck(message.f0, false);
		} else if (acknowledgeMode == AcknowledgeMode.CHECKPOINT) {
			polledAndUnacknowledgedMessageIds.add(message.f0);
		}

		return collector.hasUnpolledMessages() ? InputStatus.MORE_AVAILABLE : InputStatus.NOTHING_AVAILABLE;
	}


//	protected boolean addId(String uid) {
//		return true;
//	}



	/** COPIED FROM OLD CONNECTOR
	 * Special collector for RMQ messages.
	 * Captures the correlation ID and delivery tag also does the filtering logic for weather a message has been
	 * processed or not.
	 */
//	private class RMQCollectorImpl implements RMQDeserializationSchema.RMQCollector<T> {
//		private final SourceFunction.SourceContext<T> ctx;
//		private boolean endOfStreamSignalled = false;
//		private String correlationId;
//		private long deliveryTag;
//		private boolean customIdentifiersSet = false;
//
//		private RMQCollectorImpl(SourceFunction.SourceContext<T> ctx) {
//			this.ctx = ctx;
//		}
//
//		@Override
//		public void collect(T record) {
//			if (!customIdentifiersSet) {
//				boolean newMessage = setMessageIdentifiers(correlationId, deliveryTag);
//				if (!newMessage) {
//					return;
//				}
//			}
//
//			if (isEndOfStream(record)) {
//				this.endOfStreamSignalled = true;
//				return;
//			}
//			ctx.collect(record);
//		}
//
//		public void setFallBackIdentifiers(String correlationId, long deliveryTag){
//			this.correlationId = correlationId;
//			this.deliveryTag = deliveryTag;
//			this.customIdentifiersSet = false;
//		}
//
//		@Override
//		public boolean setMessageIdentifiers(String correlationId, long deliveryTag){
//			if (customIdentifiersSet) {
//				throw new IllegalStateException("You can set only a single set of identifiers for a block of messages.");
//			}
//
//			this.customIdentifiersSet = true;
//			if (!autoAck) {
//				if (usesCorrelationId) {
//					Preconditions.checkNotNull(correlationId, "RabbitMQ source was instantiated " +
//						"with usesCorrelationId set to true yet we couldn't extract the correlation id from it !");
//					if (!addId(correlationId)) {
//						// we have already processed this message
//						return false;
//					}
//				}
//				sessionIds.add(deliveryTag);
//			}
//			return true;
//		}
//
//		boolean isEndOfStream(T record) {
//			return endOfStreamSignalled || deliveryDeserializer.isEndOfStream(record);
//		}
//
//		public boolean isEndOfStreamSignalled() {
//			return endOfStreamSignalled;
//		}
//
//		@Override
//		public void close() {
//
//		}
//	}

	@Override
	public List<EmptyPartitionSplit> snapshotState(long checkpointId) {

		// DO STUFF FOR AT-LEAST ONCE
		Tuple2<Long, List<Long>> tuple = new Tuple2<>(checkpointId, polledAndUnacknowledgedMessageIds);
		polledAndUnacknowledgedMessageIdsPerCheckpoint.add(tuple);
		polledAndUnacknowledgedMessageIds = new ArrayList<>();
		System.out.println("Create Checkpoint: " + checkpointId);

		// DO STUFF FOR EXACTLY ONCE
		// we only need to checkpoint correlation ids
		return new ArrayList<>();
	}

	@Override
	public CompletableFuture<Void> isAvailable() {
		return FutureCompletingBlockingQueue.AVAILABLE;
	}

	@Override
	public void addSplits(List<EmptyPartitionSplit> list) {
	}

	@Override
	public void notifyNoMoreSplits() {

	}

	@Override
	public void handleSourceEvents(SourceEvent sourceEvent) {

	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		if (acknowledgeMode != AcknowledgeMode.CHECKPOINT) {
			return;
		}

		LOG.debug("Acknowledging ids for checkpoint {}", checkpointId);
		Iterator<Tuple2<Long, List<Long>>> checkpointIterator = polledAndUnacknowledgedMessageIdsPerCheckpoint.iterator();
		while (checkpointIterator.hasNext()) {
			final Tuple2<Long, List<Long>> nextCheckpoint = checkpointIterator.next();
			long nextCheckpointId = nextCheckpoint.f0;
			if (nextCheckpointId <= checkpointId) {
				acknowledgeSessionIDs(nextCheckpoint.f1);
				// remove correlation ids from global correlation ids by nextCheckpoint.f1
				checkpointIterator.remove();
			}
		}
	}

	protected void acknowledgeSessionIDs(List<Long> sessionIds) {
		try {
			for (long id : sessionIds) {
				rmqChannel.basicAck(id, false);
			}
			rmqChannel.txCommit();
		} catch (IOException e) {
			throw new RuntimeException("Messages could not be acknowledged during checkpoint creation.", e);
		}
	}

	@Override
	public void notifyCheckpointAborted(long checkpointId) throws Exception {

	}


	@Override
	public void close() throws Exception {

	}
}
