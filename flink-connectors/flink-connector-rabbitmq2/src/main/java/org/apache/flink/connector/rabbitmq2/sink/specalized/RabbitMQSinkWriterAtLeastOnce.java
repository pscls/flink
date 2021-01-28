package org.apache.flink.connector.rabbitmq2.sink.specalized;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.Connection;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.rabbitmq2.sink.RabbitMQSinkState;
import org.apache.flink.connector.rabbitmq2.sink.RabbitMQSinkWriterBase;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class RabbitMQSinkWriterAtLeastOnce<T> extends RabbitMQSinkWriterBase<T> {
    protected final ConcurrentNavigableMap<Long, byte[]> outstandingConfirms;
    private final Timer timer;
    private int counter = 0;

    public RabbitMQSinkWriterAtLeastOnce(
            RMQConnectionConfig connectionConfig,
            String queueName,
            SerializationSchema<T> serializationSchema,
            List<RabbitMQSinkState> states) {
        super(connectionConfig, queueName, serializationSchema);
        this.outstandingConfirms = new ConcurrentSkipListMap<>();
        this.timer = new Timer();

        initWithState(states);
        setupSendingTask();
    }

    private void initWithState(List<RabbitMQSinkState> states) {
        System.out.println("Init with state");
        for (RabbitMQSinkState state : states) {
            for (byte[] message : state.getOutstandingMessages()) {
                SimpleStringSchema schema = new SimpleStringSchema();
                String m = schema.deserialize(message);
                System.out.println("Resend Message from Checkpoint: " + m);
                send(message);
            }
        }
    }

    private void setupSendingTask () {
        timer.scheduleAtFixedRate(new SendMessagesTask(), 1000,5000);
    }

    class SendMessagesTask extends TimerTask {
        private Set<Long> lastSeenMessageIds;

        public SendMessagesTask() {
            super();
            this.lastSeenMessageIds = new HashSet<>();
        }

        public void run() {
            System.out.println("Run Send Messages Task");
            counter++;
            Set<Long> temp = outstandingConfirms.keySet();
            Set<Long> messagesToResend = new HashSet<>(temp);
            messagesToResend.retainAll(lastSeenMessageIds);
            for (Long id : messagesToResend) {
                if (send(outstandingConfirms.get(id))) {
                    outstandingConfirms.remove(id);
                }
            }
            lastSeenMessageIds = temp;
        }
    }

    @Override
    protected boolean send(byte[] msg) {
//        if (msg == null) return false;

        SimpleStringSchema schema = new SimpleStringSchema();
        String message = schema.deserialize(msg);
        System.out.println("Try to send:" + message);
//        System.out.println(outstandingConfirms);
        if (counter > 2) {
            throw new FlinkRuntimeException("Failed successfully");
        }
        long sequenceNumber = rmqChannel.getNextPublishSeqNo();
        outstandingConfirms.put(sequenceNumber, msg);
        return super.send(msg);

    }

    protected Channel setupChannel(Connection rmqConnection) throws IOException {
        Channel channel = super.setupChannel(rmqConnection);

        ConfirmCallback cleanOutstandingConfirms = (sequenceNumber, multiple) -> {
            SimpleStringSchema schema = new SimpleStringSchema();
            String message = schema.deserialize(outstandingConfirms.get(sequenceNumber));
            if (message.equals("Mapped: Message 1")) {
                System.out.println("Skip Message");
                return;
            }

            if (multiple) {
                ConcurrentNavigableMap<Long, byte[]> confirmed = outstandingConfirms.headMap(
                        sequenceNumber, true
                );
                confirmed.clear();
            } else {
                outstandingConfirms.remove(sequenceNumber);
            }
        };

        ConfirmCallback nackedConfirms = (sequenceNumber, multiple) -> {
            byte[] body = outstandingConfirms.get(sequenceNumber);
            System.err.format(
                    "Message with body %s has been nack-ed. Sequence number: %d, multiple: %b",
                    body, sequenceNumber, multiple
            );
            cleanOutstandingConfirms.handle(sequenceNumber, multiple);
        };

        channel.addConfirmListener(cleanOutstandingConfirms, nackedConfirms);

        channel.confirmSelect();
        return channel;
    }

    @Override
    public List<RabbitMQSinkState> snapshotState() throws IOException {
        System.out.println("Store " + outstandingConfirms.values().stream().toArray().length + " message into checkpoint.");
        return Collections.singletonList(new RabbitMQSinkState(outstandingConfirms.values()));
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (timer != null) {
            timer.cancel();
            timer.purge();
        }
    }
}
