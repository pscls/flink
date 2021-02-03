package org.apache.flink.connector.rabbitmq2.sink.writer.specalized;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.rabbitmq2.sink.RabbitMQSinkPublishOptions;
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

public class RabbitMQSinkWriterAtLeastOnce<T> extends RabbitMQSinkWriterBase<T> {
    protected final ConcurrentNavigableMap<Long, SinkMessage<T>> outstandingConfirms;
    private Set<Long> lastSeenMessageIds;

    public RabbitMQSinkWriterAtLeastOnce(
            RMQConnectionConfig connectionConfig,
            String queueName,
            SerializationSchema<T> serializationSchema,
            RabbitMQSinkPublishOptions<T> publishOptions,
            int maxRetry,
            SerializableReturnListener returnListener,
            List<RabbitMQSinkWriterState<T>> states) {
        super(connectionConfig, queueName, serializationSchema, publishOptions, maxRetry, returnListener);
        this.outstandingConfirms = new ConcurrentSkipListMap<>();
        this.lastSeenMessageIds = new HashSet<>();
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
        for (Long id : messagesToResend) {
            // remove the old message from the map, since the message was added a second time
            // under a new id or is put into the list of messages to resend
            send(outstandingConfirms.remove(id));
        }
        lastSeenMessageIds = temp;
    }

    @Override
    protected boolean send(SinkMessage<T> msg) {
        long sequenceNumber = rmqChannel.getNextPublishSeqNo();
        if (super.send(msg)) {
            outstandingConfirms.put(sequenceNumber, msg);
            return true;
        } else {
            // TODO: put in resend list
            return false;
        }
    }

    protected Channel setupChannel(Connection rmqConnection) throws IOException {
        Channel channel = super.setupChannel(rmqConnection);

        ConfirmCallback cleanOutstandingConfirms = (sequenceNumber, multiple) -> {
//            SimpleStringSchema schema = new SimpleStringSchema();
//            String message = schema.deserialize(outstandingConfirms.get(sequenceNumber));
//            if (message.equals("Mapped: Message 1")) {
//                System.out.println("Skip Message");
//                return;
//            }

            if (multiple) {
                ConcurrentNavigableMap<Long, SinkMessage<T>> confirmed = outstandingConfirms.headMap(
                        sequenceNumber, true
                );
                confirmed.clear();
            } else {
                outstandingConfirms.remove(sequenceNumber);
            }
        };

        ConfirmCallback nackedConfirms = (sequenceNumber, multiple) -> {
            SinkMessage<T> message = outstandingConfirms.get(sequenceNumber);
            System.err.format(
                    "Message with body %s has been nack-ed. Sequence number: %d, multiple: %b",
                    message, sequenceNumber, multiple
            );
            // TODO: Decide what to do here, e.g. put in messages to resend list
//            cleanOutstandingConfirms.handle(sequenceNumber, multiple);
        };

        channel.addConfirmListener(cleanOutstandingConfirms, nackedConfirms);
        channel.confirmSelect();
        return channel;
    }

    @Override
    public List<RabbitMQSinkWriterState<T>> snapshotState() throws IOException {
        // TODO: think about minimizing the resent loop by using the process time and check when the last
        // resend was executed (time difference)
        System.out.println("Outstanding confirms before resend: " + outstandingConfirms.values().stream().toArray().length);
        resendMessages();
        System.out.println("Store " + outstandingConfirms.values().stream().toArray().length + " message into checkpoint.");
        return Collections.singletonList(new RabbitMQSinkWriterState<>(new ArrayList<>(outstandingConfirms.values())));
    }
}
