package org.apache.flink.connector.rabbitmq2.sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.rabbitmq2.source.reader.RabbitMQSourceReaderBase;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

public abstract class RabbitMQSinkWriterBase<T> implements SinkWriter<T, RabbitMQSinkCommittable, RabbitMQSinkState> {
    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQSourceReaderBase.class);

    protected final RMQConnectionConfig connectionConfig;
    protected final String queueName;
    protected Connection rmqConnection;
    protected Channel rmqChannel;
    protected final SerializationSchema<T> serializationSchema;


    public RabbitMQSinkWriterBase(RMQConnectionConfig connectionConfig, String queueName, SerializationSchema<T> serializationSchema) {
        this.connectionConfig = connectionConfig;
        this.queueName = queueName;
        this.serializationSchema = serializationSchema;

        setupRabbitMQ();
    }


    protected boolean send(byte[] msg) {
//        if (msg == null) return false;
        try {
            rmqChannel.basicPublish("", queueName, null, msg);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
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
        return rmqChannel;
    }

    @Override
    public List<RabbitMQSinkCommittable> prepareCommit(boolean flush) throws IOException {
        System.out.println("Prepare Commit");
        return Collections.singletonList(new RabbitMQSinkCommittable());
    }

    @Override
    public List<RabbitMQSinkState> snapshotState() throws IOException {
        System.out.println("Base Checkpointing!!!!");
        return new ArrayList<>();
    }

    @Override
    public void close() throws Exception {
        try {
            if (rmqChannel != null) {
                rmqChannel.close();
            }
        } catch (IOException e) {
            throw new RuntimeException("Error while closing RMQ channel with " + queueName
                    + " at " + connectionConfig.getHost(), e);
        }

        try {
            if (rmqConnection != null) {
                rmqConnection.close();
            }
        } catch (IOException e) {
            throw new RuntimeException("Error while closing RMQ channel with " + queueName
                    + " at " + connectionConfig.getHost(), e);
        }
    }
}
