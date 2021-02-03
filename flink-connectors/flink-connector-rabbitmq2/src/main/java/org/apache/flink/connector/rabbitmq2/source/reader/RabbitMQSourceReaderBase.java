package org.apache.flink.connector.rabbitmq2.source.reader;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.rabbitmq2.source.common.Message;
import org.apache.flink.connector.rabbitmq2.source.split.RabbitMQPartitionSplit;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.streaming.connectors.rabbitmq.RMQDeserializationSchema;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class RabbitMQSourceReaderBase<T> implements SourceReader<T, RabbitMQPartitionSplit> {
	private static final Logger LOG = LoggerFactory.getLogger(RabbitMQSourceReaderBase.class);

	private RabbitMQPartitionSplit split;
	private final RabbitMQCollector<T> collector;
	private Connection rmqConnection;
	private Channel rmqChannel;
	private final SourceReaderContext sourceReaderContext;
	private final DeserializationSchema<T> deliveryDeserializer;

	public RabbitMQSourceReaderBase(
		SourceReaderContext sourceReaderContext,
		DeserializationSchema<T> deliveryDeserializer) {
		this.sourceReaderContext = sourceReaderContext;
		this.deliveryDeserializer = deliveryDeserializer;
		this.collector = new RabbitMQCollector<>();
	}

	@Override
	public void start() {
		sourceReaderContext.sendSplitRequest();
		System.out.println("Starting Source Reader");
	}

	protected abstract boolean isAutoAck();

	protected void setupRabbitMQ () {
		try {
			rmqConnection = setupConnection();
			rmqChannel = setupChannel(rmqConnection);
			LOG.info("RabbitMQ Connection was successful: Waiting for messages from the queue. To exit press CTRL+C");
		} catch (IOException | TimeoutException e) {
			LOG.error(e.getMessage());
		}
	}

	protected void handleMessageReceivedCallback(String consumerTag, Delivery delivery) throws IOException {
		AMQP.BasicProperties properties = delivery.getProperties();
		byte[] body = delivery.getBody();
		Envelope envelope = delivery.getEnvelope();
		collector.setFallBackIdentifiers(properties.getCorrelationId(), envelope.getDeliveryTag());
		deliveryDeserializer.deserialize(body, collector);
	}

	protected void handleMessagePolled(Message<T> message) {}

	protected Connection setupConnection() throws IOException, TimeoutException{
		final ConnectionFactory connectionFactory = new ConnectionFactory();
		connectionFactory.setHost(getSplit().getConnectionConfig().getHost());

		return connectionFactory.newConnection();
	}

	protected Channel setupChannel(Connection rmqConnection) throws IOException {
		final Channel rmqChannel = rmqConnection.createChannel();
		rmqChannel.queueDeclare(split.getQueueName(), true, false, false, null);

		// Set maximum of unacknowledged messages
		if (getSplit().getConnectionConfig().getPrefetchCount().isPresent()) {
			// global: false - the prefetch count is set per consumer, not per rabbitmq channel
			rmqChannel.basicQos(getSplit().getConnectionConfig().getPrefetchCount().get(), false);
		}

		final DeliverCallback deliverCallback = this::handleMessageReceivedCallback;
		rmqChannel.basicConsume(split.getQueueName(), isAutoAck(), deliverCallback, consumerTag -> {});
		return rmqChannel;
	}

	@Override
	public InputStatus pollNext(ReaderOutput<T> output) {
		Message<T> message = collector.pollMessage();

		if (message == null) {
			return InputStatus.NOTHING_AVAILABLE;
		}

		output.collect(message.getMessage()); //TODO: maybe we want to emit a timestamp as well?
		handleMessagePolled(message);

		return collector.hasUnpolledMessages() ? InputStatus.MORE_AVAILABLE : InputStatus.NOTHING_AVAILABLE;
	}

	@Override
	public List<RabbitMQPartitionSplit> snapshotState(long checkpointId) {
		return split != null ? Collections.singletonList(split) : new ArrayList<>();
	}

	@Override
	public CompletableFuture<Void> isAvailable() {
		return FutureCompletingBlockingQueue.AVAILABLE;
	}

	@Override
	public void addSplits(List<RabbitMQPartitionSplit> list) {
		assert list.size() == 1;
		split = list.get(0);
		setupRabbitMQ();
	}

	@Override
	public void notifyNoMoreSplits() {
	}

	@Override
	public void handleSourceEvents(SourceEvent sourceEvent) {

	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) {

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
		acknowledgeMessageIds(Collections.singletonList(id));
	}

	@Override
	public void notifyCheckpointAborted(long checkpointId) {

	}

	@Override
	public void close() throws Exception {
		if (getSplit() == null) {
			return;
		}

		try {
			if (rmqChannel != null) {
				rmqChannel.close();
			}
		} catch (IOException e) {
			throw new RuntimeException("Error while closing RMQ channel with " + split.getQueueName()
				+ " at " + getSplit().getConnectionConfig().getHost(), e);
		}

		try {
			if (rmqConnection != null) {
				rmqConnection.close();
			}
		} catch (IOException e) {
			throw new RuntimeException("Error while closing RMQ connection with " + split.getQueueName()
				+ " at " + getSplit().getConnectionConfig().getHost(), e);
		}
	}

	protected Channel getRmqChannel() {
		return rmqChannel;
	}

	protected RabbitMQPartitionSplit getSplit() { return split; }
}
