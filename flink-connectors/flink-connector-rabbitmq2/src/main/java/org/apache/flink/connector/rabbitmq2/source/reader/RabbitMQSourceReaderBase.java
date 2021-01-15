package org.apache.flink.connector.rabbitmq2.source.reader;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.rabbitmq2.source.common.EmptyPartitionSplit;
import org.apache.flink.connector.rabbitmq2.source.common.Message;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RabbitMQSourceReaderBase<T> implements SourceReader<T, EmptyPartitionSplit> {
	private static final Logger LOG = LoggerFactory.getLogger(RabbitMQSourceReaderBase.class);

	private final RMQConnectionConfig rmqConnectionConfig;
	private final String rmqQueueName;
	private final RabbitMQCollector<T> collector;
	private Connection rmqConnection;
	private Channel rmqChannel;

	public RabbitMQSourceReaderBase(
		RMQConnectionConfig rmqConnectionConfig,
		String rmqQueueName,
		DeserializationSchema<T> deliveryDeserializer) {
		this.rmqConnectionConfig = rmqConnectionConfig;
		this.rmqQueueName = rmqQueueName;
		this.collector = new RabbitMQCollector<>(deliveryDeserializer);
	}

	@Override
	public void start() {
		System.out.println("Starting Source Reader");
		setupRabbitMQ();
	}

	protected abstract boolean isAutoAck();

	private void setupRabbitMQ () {
		try {
			rmqConnection = setupConnection();
			rmqChannel = setupChannel(rmqConnection);
			LOG.info("RabbitMQ Connection was successful: Waiting for messages from the queue. To exit press CTRL+C");
		} catch (IOException | TimeoutException e) {
			LOG.error(e.getMessage());
		}
	}

	protected void handleMessageReceivedCallback(String consumerTag, Delivery delivery) throws IOException {
		collector.processMessage(delivery);
	}

	protected void handleMessagePolled(Message<T> message) {

	}

	private Connection setupConnection() throws IOException, TimeoutException{
		final ConnectionFactory connectionFactory = new ConnectionFactory();
		connectionFactory.setHost(rmqConnectionConfig.getHost());

		return connectionFactory.newConnection();
	}

	protected Channel setupChannel(Connection rmqConnection) throws IOException {
		final Channel rmqChannel = rmqConnection.createChannel();
		rmqChannel.queueDeclare(rmqQueueName, true, false, false, null);

		// Set maximum of unacknowledged messages
		if (rmqConnectionConfig.getPrefetchCount().isPresent()) {
			// global: false - the prefetch count is set per consumer, not per rabbitmq channel
			rmqChannel.basicQos(rmqConnectionConfig.getPrefetchCount().get(), false);
		}

		// if masterSplit.hasIds()
		// ack all ids from masterSplit

		final DeliverCallback deliverCallback = this::handleMessageReceivedCallback;
		rmqChannel.basicConsume(rmqQueueName, isAutoAck(), deliverCallback, consumerTag -> {});
		return rmqChannel;
	}

	@Override
	public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
		Message<T> message = collector.getMessage();

		if (message == null) {
			return InputStatus.NOTHING_AVAILABLE;
		}

		output.collect(message.message); //TODO: maybe we want to emit a timestamp as well?
		handleMessagePolled(message);

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

	}

	protected void acknowledgeMessageIds(List<Long> sessionIds) {
		try {
			for (long id : sessionIds) {
				rmqChannel.basicAck(id, false);
			}
		} catch (IOException e) {
			throw new RuntimeException("Messages could not be acknowledged during checkpoint creation.", e);
		}
	}

	protected void acknowledgeMessageId(long id) {
		try {
			rmqChannel.basicAck(id, false);
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

	protected Channel getRmqChannel() {
		return rmqChannel;
	}
}
