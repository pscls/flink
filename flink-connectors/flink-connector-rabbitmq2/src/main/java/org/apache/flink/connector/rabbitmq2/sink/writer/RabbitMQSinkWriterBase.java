package org.apache.flink.connector.rabbitmq2.sink.writer;

import akka.stream.javadsl.Sink;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.rabbitmq2.sink.RabbitMQSinkPublishOptions;
import org.apache.flink.connector.rabbitmq2.sink.SinkMessage;
import org.apache.flink.connector.rabbitmq2.sink.state.RabbitMQSinkWriterState;
import org.apache.flink.connector.rabbitmq2.source.reader.RabbitMQSourceReaderBase;
import org.apache.flink.streaming.connectors.rabbitmq.SerializableReturnListener;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

public abstract class RabbitMQSinkWriterBase<T> implements SinkWriter<T, Void, RabbitMQSinkWriterState<T>> {
    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQSourceReaderBase.class);

    protected final RMQConnectionConfig connectionConfig;
    protected final String queueName;
    protected Connection rmqConnection;
    protected Channel rmqChannel;
    protected final SerializationSchema<T> serializationSchema;
    protected final int maxRetry;

    @Nullable private final RabbitMQSinkPublishOptions<T> publishOptions;

    @Nullable private final SerializableReturnListener returnListener;


    public RabbitMQSinkWriterBase(RMQConnectionConfig connectionConfig, String queueName, SerializationSchema<T> serializationSchema, RabbitMQSinkPublishOptions<T> publishOptions, int maxRetry, SerializableReturnListener returnListener) {
        this.connectionConfig = connectionConfig;
        this.queueName = queueName;
        this.serializationSchema = serializationSchema;
        this.publishOptions = publishOptions;
        this.maxRetry = maxRetry;
        this.returnListener = returnListener;
        setupRabbitMQ();
    }

    protected boolean send(SinkMessage<T> message) {
        try {
            message.addRetries();
            byte[] value = message.getBytes(serializationSchema);
            if (publishOptions == null) {
                rmqChannel.basicPublish("", queueName, null, value);
            } else {
                T msg = message.getMessage(publishOptions.getDeserializationSchema());
                boolean mandatory = publishOptions.computeMandatory(msg);
                boolean immediate = publishOptions.computeImmediate(msg);

                Preconditions.checkState(
                        !(returnListener == null && (mandatory || immediate)),
                        "Setting mandatory and/or immediate flags to true requires a ReturnListener.");

                String rk = publishOptions.computeRoutingKey(msg);
                String exchange = publishOptions.computeExchange(msg);

                rmqChannel.basicPublish(
                        exchange,
                        rk,
                        mandatory,
                        immediate,
                        publishOptions.computeProperties(msg),
                        value);
            }
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public void write(T element, Context context) throws IOException {
        send(new SinkMessage<>(element));
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
    public List<Void> prepareCommit(boolean flush) throws IOException {
        System.out.println("Prepare Commit");
        return new ArrayList<>();
    }

    @Override
    public List<RabbitMQSinkWriterState<T>> snapshotState() throws IOException {
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
