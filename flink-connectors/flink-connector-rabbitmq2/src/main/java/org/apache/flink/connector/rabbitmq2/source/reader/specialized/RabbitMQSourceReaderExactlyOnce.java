package org.apache.flink.connector.rabbitmq2.source.reader.specialized;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.rabbitmq2.source.common.Message;
import org.apache.flink.connector.rabbitmq2.source.reader.RabbitMQSourceReaderBase;
import org.apache.flink.connector.rabbitmq2.source.split.RabbitMQPartitionSplit;
import org.apache.flink.util.Preconditions;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * TODO.
 *
 * @param <T>
 */
public class RabbitMQSourceReaderExactlyOnce<T> extends RabbitMQSourceReaderBase<T> {
    private List<Message<T>> polledAndUnacknowledgedMessagesSinceLastCheckpoint;
    private final Deque<Tuple2<Long, List<Message<T>>>>
            polledAndUnacknowledgedMessagesPerCheckpoint;
    private final ConcurrentHashMap.KeySetView<String, Boolean> correlationIds;

    public RabbitMQSourceReaderExactlyOnce(
            SourceReaderContext sourceReaderContext,
            DeserializationSchema<T> deliveryDeserializer) {
        super(sourceReaderContext, deliveryDeserializer);
        this.polledAndUnacknowledgedMessagesSinceLastCheckpoint = new ArrayList<>();
        this.polledAndUnacknowledgedMessagesPerCheckpoint = new ArrayDeque<>();
        this.correlationIds = ConcurrentHashMap.newKeySet();
    }

    @Override
    protected boolean isAutoAck() {
        return false;
    }

    @Override
    protected void handleMessagePolled(Message<T> message) {
        this.polledAndUnacknowledgedMessagesSinceLastCheckpoint.add(message);
    }

    @Override
    protected void handleMessageReceivedCallback(String consumerTag, Delivery delivery)
            throws IOException {
        AMQP.BasicProperties properties = delivery.getProperties();
        String correlationId = properties.getCorrelationId();
        Preconditions.checkNotNull(
                correlationId,
                "RabbitMQ source was instantiated "
                        + "with consistencyMode set EXACTLY_ONCE yet we couldn't extract the correlation id from it !");

        Envelope envelope = delivery.getEnvelope();
        long deliveryTag = envelope.getDeliveryTag();
        // handle this message only if we haven't seen the correlation id before
        // otherwise, store the new delivery-tag for later acknowledgments

        System.out.println(correlationId);
        System.out.println(correlationIds);
        if (correlationIds.contains(correlationId)) {
            System.out.println("============ saw this message before:" + deliveryTag);
            polledAndUnacknowledgedMessagesSinceLastCheckpoint.add(
                    new Message<>(deliveryTag, correlationId));
        } else {
            super.handleMessageReceivedCallback(consumerTag, delivery);
            correlationIds.add(correlationId);
        }
    }

    @Override
    public List<RabbitMQPartitionSplit> snapshotState(long checkpointId) {
        System.out.println("Create Snapshot");
        Tuple2<Long, List<Message<T>>> tuple =
                new Tuple2<>(checkpointId, polledAndUnacknowledgedMessagesSinceLastCheckpoint);
        polledAndUnacknowledgedMessagesPerCheckpoint.add(tuple);

        polledAndUnacknowledgedMessagesSinceLastCheckpoint = new ArrayList<>();

        if (getSplit() != null) {
            getSplit().setCorrelationIds(correlationIds);
        }
        return super.snapshotState(checkpointId);
    }

    @Override
    public void addSplits(List<RabbitMQPartitionSplit> list) {
        super.addSplits(list);
        correlationIds.addAll(getSplit().getCorrelationIds());
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        System.out.println("Checkpoint Complete: " + checkpointId);
        Iterator<Tuple2<Long, List<Message<T>>>> checkpointIterator =
                polledAndUnacknowledgedMessagesPerCheckpoint.iterator();
        while (checkpointIterator.hasNext()) {
            final Tuple2<Long, List<Message<T>>> nextCheckpoint = checkpointIterator.next();
            long nextCheckpointId = nextCheckpoint.f0;
            if (nextCheckpointId <= checkpointId) {
                acknowledgeMessages(nextCheckpoint.f1);
                checkpointIterator.remove();
            }
        }
    }

    @Override
    protected void setupChannel() throws IOException {
        super.setupChannel();
        // enable channel commit mode if acknowledging happens after checkpoint
        rmqChannel.txSelect();
    }

    private void acknowledgeMessages(List<Message<T>> messages) {
        List<String> correlationIds =
                messages.stream().map(Message::getCorrelationId).collect(Collectors.toList());
        // TODO: Find a good compromise when to remove the correlations Ids from
        this.correlationIds.removeAll(correlationIds);
        try {
            List<Long> deliveryTags =
                    messages.stream().map(Message::getDeliveryTag).collect(Collectors.toList());
            acknowledgeMessageIds(deliveryTags);
            getRmqChannel().txCommit();
            LOG.info("Successfully acknowledged " + deliveryTags.size() + " messages.");
        } catch (IOException e) {
            LOG.error(
                    "Error during acknowledgement of "
                            + correlationIds.size()
                            + " messages. CorrelationIds will be rolled back. Error: "
                            + e.getMessage());
            this.correlationIds.addAll(correlationIds);
            e.printStackTrace();
        }
    }
}
