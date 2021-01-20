package org.apache.flink.connector.rabbitmq2.sink;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import com.rabbitmq.client.DeliverCallback;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.rabbitmq2.source.reader.RabbitMQSourceReaderBase;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import org.eclipse.jetty.server.session.HashSessionIdManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;

public class RabbitMQSinkWriter<T> implements SinkWriter<T, RabbitMQSinkCommitable, RabbitMQSinkBucketState> {
    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQSourceReaderBase.class);

    private final RMQConnectionConfig connectionConfig;
    private final String queueName;
    private Connection rmqConnection;
    private Channel rmqChannel;
    private final SerializationSchema<T> serializationSchema;
    private final ConcurrentNavigableMap<Long, byte[]> outstandingConfirms = new ConcurrentSkipListMap<>();
    private final Timer timer;

    public RabbitMQSinkWriter(RMQConnectionConfig connectionConfig, String queueName, SerializationSchema<T> serializationSchema) {
        this.connectionConfig = connectionConfig;
        this.queueName = queueName;
        this.serializationSchema = serializationSchema;
        this.timer = new Timer();

        setupRabbitMQ();
        setupSendingTask();
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
            Set<Long> temp = outstandingConfirms.keySet();
            Set<Long> messagesToResend = new HashSet<>(temp);
            messagesToResend.retainAll(lastSeenMessageIds);
            for (Long id : messagesToResend) {
                try {
                    send(outstandingConfirms.get(id));
                    outstandingConfirms.remove(id);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            lastSeenMessageIds = temp;
        }
    }

    private void send(byte[] msg) throws IOException {
        if (msg == null) return;

        long sequenceNumber = rmqChannel.getNextPublishSeqNo();
        outstandingConfirms.put(sequenceNumber, msg);
        rmqChannel.basicPublish("", queueName, null, msg);
    }

    @Override
    public void write(T element, Context context) throws IOException {
        byte[] msg = serializationSchema.serialize(element);
        send(msg);
    }

    protected void setupRabbitMQ () {
        try {
            rmqConnection = setupConnection();
            rmqChannel = setupChannel(rmqConnection);
            LOG.info("RabbitMQ Connection was successful: Waiting for messages from the queue. To exit press CTRL+C");
        } catch (IOException | TimeoutException e) {
            LOG.error(e.getMessage());
        }
    }

    protected Connection setupConnection() throws IOException, TimeoutException {
        final ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(connectionConfig.getHost());
        return connectionFactory.newConnection();
    }

    protected Channel setupChannel(Connection rmqConnection) throws IOException {
        final Channel rmqChannel = rmqConnection.createChannel();
        rmqChannel.queueDeclare(queueName, true, false, false, null);

        ConfirmCallback cleanOutstandingConfirms = (sequenceNumber, multiple) -> {
//            if (sequenceNumber % 2 == 0) {
//                return;
//            }
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

        rmqChannel.addConfirmListener(cleanOutstandingConfirms, nackedConfirms);

        rmqChannel.confirmSelect();
        return rmqChannel;
    }

    @Override
    public List<RabbitMQSinkCommitable> prepareCommit(boolean flush) throws IOException {
        return new ArrayList<>();
    }

    @Override
    public List<RabbitMQSinkBucketState> snapshotState() throws IOException {
        return new ArrayList<>();
    }

    @Override
    public void close() throws Exception {
        if (timer != null) {
            timer.cancel();
            timer.purge();
        }
    }
}
