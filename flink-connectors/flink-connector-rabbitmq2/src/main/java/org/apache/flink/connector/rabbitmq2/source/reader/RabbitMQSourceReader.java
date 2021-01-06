package org.apache.flink.connector.rabbitmq2.source.reader;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.rabbitmq2.source.common.EmptyPartitionSplit;
import org.apache.flink.core.io.InputStatus;
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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RabbitMQSourceReader<T> implements SourceReader<T, EmptyPartitionSplit> {
	private static final Logger LOG = LoggerFactory.getLogger(RabbitMQSourceReader.class);

	private final RMQConnectionConfig rmqConnectionConfig;
	private final String rmqQueueName;
	private final RabbitMQCollector collector;
	private Connection rmqConnection;
	private Channel rmqChannel;

	protected transient boolean autoAck = false;
	private final boolean usesCorrelationId = false;
	protected transient List<Long> sessionIds = new ArrayList<>(64);

	public RabbitMQSourceReader(
		RMQConnectionConfig rmqConnectionConfig,
		String rmqQueueName,
		DeserializationSchema<T> deliveryDeserializer) {
		this.rmqConnectionConfig = rmqConnectionConfig;
		this.rmqQueueName = rmqQueueName;
//		this.deliveryDeserializer = deliveryDeserializer;
		this.collector = new RabbitMQCollector(deliveryDeserializer);
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

			final DeliverCallback deliverCallback = (consumerTag, delivery) -> collector.processMessage(delivery);

			rmqChannel.basicConsume(rmqQueueName, false, deliverCallback, consumerTag -> {});
			System.out.println("Waiting for messages from the queue. To exit press CTRL+C");
		} catch (IOException | TimeoutException e) {
			System.out.println("[ERROR]" + e.getMessage());
			// log error
		}
	}

	@Override
	public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
		Tuple2<Long, T> message = collector.getMessage();

		if (message == null) {
			return InputStatus.NOTHING_AVAILABLE;
		}

		output.collect(message.f1); //TODO: maybe we want to emit a timestamp as well?
		collector.emittedAndUnacknowledgedMessageIds.add(message.f0);

		return collector.queue.size() > 0 ? InputStatus.MORE_AVAILABLE : InputStatus.NOTHING_AVAILABLE;
	}

	private class RabbitMQCollector {
		private final Queue<Tuple2<Long, T>> queue;
		private List<Long> emittedAndUnacknowledgedMessageIds;
		private final DeserializationSchema<T> deliveryDeserializer;

		private RabbitMQCollector(DeserializationSchema<T> deliveryDeserializer) {
			this.deliveryDeserializer = deliveryDeserializer;
			this.queue = new LinkedList<>();
			this.emittedAndUnacknowledgedMessageIds = new ArrayList<>();
		}

		// copied from old rmq connector
		private void processMessage(Delivery delivery)
			throws IOException {
//			AMQP.BasicProperties properties = delivery.getProperties();
			byte[] body = delivery.getBody();
			Envelope envelope = delivery.getEnvelope();
			long deliveryTag = envelope.getDeliveryTag();
//		collector.setFallBackIdentifiers(properties.getCorrelationId(), envelope.getDeliveryTag());
			T message = deliveryDeserializer.deserialize(body);
			queue.add(new Tuple2<>(deliveryTag, message));
		}

		private Tuple2<Long, T> getMessage() {
			return queue.poll();
		}
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
		acknowledgeSessionIDs(collector.emittedAndUnacknowledgedMessageIds);
		collector.emittedAndUnacknowledgedMessageIds = new ArrayList<>();
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
