package org.apache.flink.connector.rabbitmq2.source.reader;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;

import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.rabbitmq2.source.split.RabbitMQPartitionSplit;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.rabbitmq.RMQDeserializationSchema;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RabbitMQSourceReader<T> implements SourceReader<T, RabbitMQPartitionSplit> {
	private static final Logger LOG = LoggerFactory.getLogger(RabbitMQSourceReader.class);

	private final RMQConnectionConfig rmqConnectionConfig;
	private final String rmqQueueName;
	private final Queue<T> queue;
	private final List<Integer> emittedAndUnacknowledgedMessageIds;
	private Connection rmqConnection;
	private Channel rmqChannel;
	private final RMQDeserializationSchema<T> deliveryDeserializer;
	protected transient boolean autoAck = false;
	private final boolean usesCorrelationId = false;
	protected transient List<Long> sessionIds = new ArrayList<Long>(64);

	public RabbitMQSourceReader(
		RMQConnectionConfig rmqConnectionConfig,
		String rmqQueueName,
		RMQDeserializationSchema<T> deliveryDeserializer) {
		this.rmqConnectionConfig = rmqConnectionConfig;
		this.rmqQueueName = rmqQueueName;
		this.queue = new LinkedList<T>();
		this.emittedAndUnacknowledgedMessageIds = new ArrayList<>();
		this.deliveryDeserializer = deliveryDeserializer;
	}

	@Override
	public void start() {
		System.out.println("Starting Source Reader");
		final ConnectionFactory connectionFactory = new ConnectionFactory();
		connectionFactory.setHost(rmqConnectionConfig.getHost());

		try {
			rmqConnection = connectionFactory.newConnection();
			rmqChannel = rmqConnection.createChannel();
			rmqChannel.queueDeclare(rmqQueueName, true, false, false, null);
			// Maximum of unacknowledged messages
			rmqChannel.basicQos(10, true);

			final DeliverCallback deliverCallback = (consumerTag, delivery) -> {
				long tag = delivery.getEnvelope().getDeliveryTag();
				System.out.println(tag);
				final String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
				queue.add((T) message);
				System.out.println(
					"Received from message from the queue: " + message + " Id: (" + tag + ")");
			};

			rmqChannel.basicConsume(rmqQueueName, false, deliverCallback, consumerTag -> {});
			System.out.println("Waiting for messages from the queue. To exit press CTRL+C");
		} catch (IOException | TimeoutException e) {
			System.out.println("[ERROR]" + e.getMessage());
			// log error
		}
	}


	@Override
	public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
		T element = queue.poll();
		if (element == null) {
			return InputStatus.NOTHING_AVAILABLE;
		}
		output.collect(element); //TODO: maybe we want to emit a timestamp as well?
		//emittedAndUnacknowledgedMessageIds.add(element.id);
		return queue.size() > 0 ? InputStatus.MORE_AVAILABLE : InputStatus.NOTHING_AVAILABLE;
	}

	protected boolean addId(String uid) {
		return true;
	}

	// copied from old rmq connector
	private void processMessage(Delivery delivery, RMQCollectorImpl collector)
		throws IOException {
		AMQP.BasicProperties properties = delivery.getProperties();
		byte[] body = delivery.getBody();
		Envelope envelope = delivery.getEnvelope();
		collector.setFallBackIdentifiers(properties.getCorrelationId(), envelope.getDeliveryTag());
		deliveryDeserializer.deserialize(envelope, properties, body, collector);
	}

	/** COPIED FROM OLD CONNECTOR
	 * Special collector for RMQ messages.
	 * Captures the correlation ID and delivery tag also does the filtering logic for weather a message has been
	 * processed or not.
	 */
	private class RMQCollectorImpl implements RMQDeserializationSchema.RMQCollector<T> {
		private final SourceFunction.SourceContext<T> ctx;
		private boolean endOfStreamSignalled = false;
		private String correlationId;
		private long deliveryTag;
		private boolean customIdentifiersSet = false;

		private RMQCollectorImpl(SourceFunction.SourceContext<T> ctx) {
			this.ctx = ctx;
		}

		@Override
		public void collect(T record) {
			if (!customIdentifiersSet) {
				boolean newMessage = setMessageIdentifiers(correlationId, deliveryTag);
				if (!newMessage) {
					return;
				}
			}

			if (isEndOfStream(record)) {
				this.endOfStreamSignalled = true;
				return;
			}
			ctx.collect(record);
		}

		public void setFallBackIdentifiers(String correlationId, long deliveryTag){
			this.correlationId = correlationId;
			this.deliveryTag = deliveryTag;
			this.customIdentifiersSet = false;
		}

		@Override
		public boolean setMessageIdentifiers(String correlationId, long deliveryTag){
			if (customIdentifiersSet) {
				throw new IllegalStateException("You can set only a single set of identifiers for a block of messages.");
			}

			this.customIdentifiersSet = true;
			if (!autoAck) {
				if (usesCorrelationId) {
					Preconditions.checkNotNull(correlationId, "RabbitMQ source was instantiated " +
						"with usesCorrelationId set to true yet we couldn't extract the correlation id from it !");
					if (!addId(correlationId)) {
						// we have already processed this message
						return false;
					}
				}
				sessionIds.add(deliveryTag);
			}
			return true;
		}

		boolean isEndOfStream(T record) {
			return endOfStreamSignalled || deliveryDeserializer.isEndOfStream(record);
		}

		public boolean isEndOfStreamSignalled() {
			return endOfStreamSignalled;
		}

		@Override
		public void close() {

		}
	}

	@Override
	public List<RabbitMQPartitionSplit> snapshotState(long checkpointId) {
		return null;
	}

	@Override
	public CompletableFuture<Void> isAvailable() {
		return FutureCompletingBlockingQueue.AVAILABLE;
	}

	@Override
	public void addSplits(List<RabbitMQPartitionSplit> splits) {

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

	@Override
	public void notifyCheckpointAborted(long checkpointId) throws Exception {

	}


	@Override
	public void close() throws Exception {

	}
}
