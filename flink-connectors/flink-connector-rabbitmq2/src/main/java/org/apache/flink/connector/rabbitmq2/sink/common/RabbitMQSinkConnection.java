package org.apache.flink.connector.rabbitmq2.sink.common;

import org.apache.flink.connector.rabbitmq2.common.RabbitMQConnectionConfig;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

public class RabbitMQSinkConnection<T> {
    protected static final Logger LOG = LoggerFactory.getLogger(RabbitMQSinkConnection.class);

    private final String queueName;
    private final Connection rmqConnection;
    private final Channel rmqChannel;

    @Nullable private final RabbitMQSinkPublishOptions<T> publishOptions;

    @Nullable private final SerializableReturnListener returnListener;

    public RabbitMQSinkConnection(
            RabbitMQConnectionConfig connectionConfig,
            String queueName,
            @Nullable RabbitMQSinkPublishOptions<T> publishOptions,
            @Nullable SerializableReturnListener returnListener)
            throws Exception {
        this.queueName = queueName;
        this.publishOptions = publishOptions;
        this.returnListener = returnListener;
        this.rmqConnection = setupConnection(connectionConfig);
        this.rmqChannel = setupChannel(rmqConnection, queueName, returnListener);
    }

    private Connection setupConnection(RabbitMQConnectionConfig connectionConfig)
            throws Exception {
        return connectionConfig.getConnectionFactory().newConnection();
    }

    private Channel setupChannel(
            Connection rmqConnection, String queueName, SerializableReturnListener returnListener)
            throws IOException {
        final Channel rmqChannel = rmqConnection.createChannel();
        rmqChannel.queueDeclare(queueName, true, false, false, null);
        if (returnListener != null) {
            rmqChannel.addReturnListener(returnListener);
        }
        return rmqChannel;
    }

    /**
     * Only used by at-least-once and exactly-once for resending messages that could not be
     * delivered.
     *
     * @param message sink message wrapping the atomic message object
     */
    protected void send(RabbitMQSinkMessageWrapper<T> message) {
        send(message.getMessage(), message.getBytes());
    }

    /**
     * Publish a message to a queue in RabbitMQ. With publish options enabled, first compute the
     * necessary publishing information.
     *
     * @param message original message, only required for publishing with publish options present
     * @param serializedMessage serialized message to send to RabbitMQ
     */
    protected void send(T message, byte[] serializedMessage) {
        try {
            if (publishOptions == null) {
                rmqChannel.basicPublish("", queueName, null, serializedMessage);
            } else {
                publishWithOptions(message, serializedMessage);
            }
        } catch (IOException e) {
            throw new FlinkRuntimeException(e.getMessage());
        }
    }

    private void publishWithOptions(T message, byte[] serializedMessage) throws IOException {
        if (publishOptions == null) {
            throw new RuntimeException("Try to publish with options without publishOptions.");
        }

        boolean mandatory = publishOptions.computeMandatory(message);
        boolean immediate = publishOptions.computeImmediate(message);

        Preconditions.checkState(
                !(returnListener == null && (mandatory || immediate)),
                "Setting mandatory and/or immediate flags to true requires a ReturnListener.");

        String rk = publishOptions.computeRoutingKey(message);
        String exchange = publishOptions.computeExchange(message);

        rmqChannel.basicPublish(
                exchange,
                rk,
                mandatory,
                immediate,
                publishOptions.computeProperties(message),
                serializedMessage);
    }

    protected void close() throws Exception {
        // close the channel
        if (rmqChannel != null) {
            rmqChannel.close();
        }

        // close the connection
        if (rmqConnection != null) {
            rmqConnection.close();
        }
    }

    protected Channel getRmqChannel() {
        return rmqChannel;
    }
}
