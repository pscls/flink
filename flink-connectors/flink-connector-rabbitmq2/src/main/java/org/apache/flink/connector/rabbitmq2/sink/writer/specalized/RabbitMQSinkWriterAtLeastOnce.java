package org.apache.flink.connector.rabbitmq2.sink.writer.specalized;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.rabbitmq2.sink.SinkMessage;
import org.apache.flink.connector.rabbitmq2.sink.state.RabbitMQSinkWriterState;
import org.apache.flink.connector.rabbitmq2.sink.writer.RabbitMQSinkWriterBase;
import org.apache.flink.streaming.connectors.rabbitmq.SerializableReturnListener;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/** TODO. */
public class RabbitMQSinkWriterAtLeastOnce<T> extends RabbitMQSinkWriterBase<T> {
    protected final ConcurrentNavigableMap<Long, SinkMessage<T>> outstandingConfirms;
    private Set<Long> lastSeenMessageIds;
    private long lastResendTimestampMilliseconds;
    private final long resendIntervalMilliseconds;

    public static final long DEFAULT_MINIMAL_RESEND_INTERVAL = 5000L;

    public RabbitMQSinkWriterAtLeastOnce(
            RMQConnectionConfig connectionConfig,
            String queueName,
            SerializationSchema<T> serializationSchema,
            RabbitMQSinkPublishOptions<T> publishOptions,
            int maxRetry,
            SerializableReturnListener returnListener,
            Long minimalResendIntervalMilliseconds,
            List<RabbitMQSinkWriterState<T>> states) {
        super(
                connectionConfig,
                queueName,
                serializationSchema,
                publishOptions,
                maxRetry,
                returnListener);
        this.outstandingConfirms = new ConcurrentSkipListMap<>();
        this.lastSeenMessageIds = new HashSet<>();
        this.lastResendTimestampMilliseconds = System.currentTimeMillis();
        this.resendIntervalMilliseconds =
                minimalResendIntervalMilliseconds != null
                        ? minimalResendIntervalMilliseconds
                        : DEFAULT_MINIMAL_RESEND_INTERVAL;
        initWithState(states);
    }

    private void initWithState(List<RabbitMQSinkWriterState<T>> states) {
        System.out.println("Init with state");
        for (RabbitMQSinkWriterState<T> state : states) {
            for (SinkMessage<T> message : state.getOutstandingMessages()) {
                send(message);
            }
        }
    }

    private void resendMessages() {
        Set<Long> temp = outstandingConfirms.keySet();
        Set<Long> messagesToResend = new HashSet<>(temp);
        messagesToResend.retainAll(lastSeenMessageIds);
        System.out.println("resend: " + messagesToResend.size());
        for (Long id : messagesToResend) {

            // remove the old message from the map, since the message was added a second time
            // under a new id or is put into the list of messages to resend
            SinkMessage<T> msg = outstandingConfirms.remove(id);
            if (msg != null) {
                send(msg);
            }
        }
        lastSeenMessageIds = temp;
    }

    @Override
    protected void send(SinkMessage<T> msg) {
        long sequenceNumber = rmqChannel.getNextPublishSeqNo();
        super.send(msg);
        outstandingConfirms.put(sequenceNumber, msg);
    }

    protected Channel setupChannel(Connection rmqConnection) throws IOException {
        Channel channel = super.setupChannel(rmqConnection);

        ConfirmCallback cleanOutstandingConfirms =
                (sequenceNumber, multiple) -> {
                    if (multiple) {
                        ConcurrentNavigableMap<Long, SinkMessage<T>> confirmed =
                                outstandingConfirms.headMap(sequenceNumber, true);
                        confirmed.clear();
                    } else {
                        outstandingConfirms.remove(sequenceNumber);
                    }
                };

        ConfirmCallback nackedConfirms =
                (sequenceNumber, multiple) -> {
                    SinkMessage<T> message = outstandingConfirms.get(sequenceNumber);
                    LOG.error(
                            "Message with body {} has been nack-ed. Sequence number: {}, multiple: {}",
                            message.getMessage(),
                            sequenceNumber,
                            multiple);
                };

        channel.addConfirmListener(cleanOutstandingConfirms, nackedConfirms);
        channel.confirmSelect();
        return channel;
    }

    @Override
    public List<RabbitMQSinkWriterState<T>> snapshotState() {
        if (System.currentTimeMillis() - lastResendTimestampMilliseconds
                > resendIntervalMilliseconds) {
            resendMessages();
            lastResendTimestampMilliseconds = System.currentTimeMillis();
        }
        System.out.println("Store " + outstandingConfirms.size() + " message into checkpoint.");
        return Collections.singletonList(
                new RabbitMQSinkWriterState<>(new ArrayList<>(outstandingConfirms.values())));
    }
}
