package org.apache.flink.connector.rabbitmq2.source.reader;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.rabbitmq2.source.common.Message;
import org.apache.flink.connector.rabbitmq2.source.split.RabbitMQPartitionSplit;
import org.apache.flink.core.io.InputStatus;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * TODO.
 *
 * @param <T>
 */
public abstract class RabbitMQSourceReaderBase<T>
        implements SourceReader<T, RabbitMQPartitionSplit> {
    protected static final Logger LOG = LoggerFactory.getLogger(RabbitMQSourceReaderBase.class);

    private RabbitMQPartitionSplit split;
    private final RabbitMQCollector<T> collector;
    private Connection rmqConnection;
    protected Channel rmqChannel;
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
        System.out.println("Starting Source Reader: " + this.hashCode());
        sourceReaderContext.sendSplitRequest();
    }

    protected abstract boolean isAutoAck();

    protected void setupRabbitMQ() {
        try {
            setupConnection();
            setupChannel();
            LOG.info(
                    "RabbitMQ Connection was successful: Waiting for messages from the queue. To exit press CTRL+C");
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    /**
     * This function will be called when a new message from rabbitmq gets pushed to the source. The
     * message will be deserialized and forwarded to our message collector where it's buffered until
     * it can be processed.
     *
     * @param consumerTag
     * @param delivery
     * @throws IOException
     */
    protected void handleMessageReceivedCallback(String consumerTag, Delivery delivery)
            throws IOException {

        // byte[] m = String.valueOf(System.currentTimeMillis()).getBytes();
        AMQP.BasicProperties properties = delivery.getProperties();
        byte[] body = delivery.getBody();
        Envelope envelope = delivery.getEnvelope();
        collector.setFallBackIdentifiers(properties.getCorrelationId(), envelope.getDeliveryTag());

        // collector.collect(deliveryDeserializer.deserialize(m));
        deliveryDeserializer.deserialize(body, collector);
    }

    protected void handleMessagePolled(Message<T> message) {}

    protected ConnectionFactory setupConnectionFactory() throws Exception {
        ConnectionFactory factory = split.getConnectionConfig().getConnectionFactory();
        factory.setAutomaticRecoveryEnabled(false);
        return factory;
    }

    protected void setupConnection() throws Exception {
        rmqConnection = setupConnectionFactory().newConnection();
    }

    protected void setupChannel() throws IOException {
        rmqChannel = rmqConnection.createChannel();
        rmqChannel.queueDeclare(split.getQueueName(), true, false, false, null);

        // Set maximum of unacknowledged messages
        if (getSplit().getConnectionConfig().getPrefetchCount().isPresent()) {
            // global: false - the prefetch count is set per consumer, not per rabbitmq channel
            rmqChannel.basicQos(getSplit().getConnectionConfig().getPrefetchCount().get(), false);
        }

        final DeliverCallback deliverCallback = this::handleMessageReceivedCallback;
        rmqChannel.basicConsume(
                split.getQueueName(), isAutoAck(), deliverCallback, consumerTag -> {});
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) {
        Message<T> message = collector.pollMessage();

        if (message == null) {
            return InputStatus.NOTHING_AVAILABLE;
        }

        //        output.collect(
        //                (T) (message.getMessage() + "-" +
        // String.valueOf(System.currentTimeMillis())));
        output.collect(message.getMessage());
        handleMessagePolled(message);

        return collector.hasUnpolledMessages()
                ? InputStatus.MORE_AVAILABLE
                : InputStatus.NOTHING_AVAILABLE;
    }

    @Override
    public List<RabbitMQPartitionSplit> snapshotState(long checkpointId) {
        return split != null ? Collections.singletonList(split.clone()) : new ArrayList<>();
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return FutureCompletingBlockingQueue.AVAILABLE;
    }

    @Override
    public void addSplits(List<RabbitMQPartitionSplit> list) {
        if (split != null) {
            return;
        }
        assert list.size() == 1;
        split = list.get(0);
        setupRabbitMQ();
    }

    @Override
    public void notifyNoMoreSplits() {
        System.out.println("No more splits");
        try {
            close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    protected void acknowledgeMessageIds(List<Long> sessionIds) {
        try {
            for (long id : sessionIds) {
                rmqChannel.basicAck(id, false);
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    "Messages could not be acknowledged during checkpoint creation.", e);
        }
    }

    protected void acknowledgeMessageId(long id) {
        acknowledgeMessageIds(Collections.singletonList(id));
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        System.out.println("Checkpoint Aborted");
    }

    @Override
    public void close() throws Exception {
        System.out.println("CLOSE READER");
        if (getSplit() == null) {
            return;
        }

        try {
            if (rmqChannel != null) {
                rmqChannel.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    "Error while closing RMQ channel with "
                            + split.getQueueName()
                            + " at "
                            + getSplit().getConnectionConfig().getHost(),
                    e);
        }

        try {
            if (rmqConnection != null) {
                rmqConnection.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    "Error while closing RMQ connection with "
                            + split.getQueueName()
                            + " at "
                            + getSplit().getConnectionConfig().getHost(),
                    e);
        }
    }

    protected Channel getRmqChannel() {
        return rmqChannel;
    }

    protected RabbitMQPartitionSplit getSplit() {
        return split;
    }
}
