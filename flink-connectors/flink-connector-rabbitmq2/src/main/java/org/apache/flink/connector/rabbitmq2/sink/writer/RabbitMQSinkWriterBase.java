/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.rabbitmq2.sink.writer;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.rabbitmq2.common.RabbitMQConnectionConfig;
import org.apache.flink.connector.rabbitmq2.sink.common.RabbitMQSinkMessageWrapper;
import org.apache.flink.connector.rabbitmq2.sink.common.RabbitMQSinkPublishOptions;
import org.apache.flink.connector.rabbitmq2.sink.common.SerializableReturnListener;
import org.apache.flink.connector.rabbitmq2.sink.state.RabbitMQSinkWriterState;
import org.apache.flink.connector.rabbitmq2.sink.writer.specialized.RabbitMQSinkWriterAtLeastOnce;
import org.apache.flink.connector.rabbitmq2.sink.writer.specialized.RabbitMQSinkWriterAtMostOnce;
import org.apache.flink.connector.rabbitmq2.sink.writer.specialized.RabbitMQSinkWriterExactlyOnce;
import org.apache.flink.util.Preconditions;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * RabbitMQSinkWriterBase is the common abstract class of {@link RabbitMQSinkWriterAtMostOnce},
 * {@link RabbitMQSinkWriterAtLeastOnce} and {@link RabbitMQSinkWriterExactlyOnce}.
 *
 * <p>This class provides basic RabbitMQ functionality and common behaviour such as establishing and
 * closing a connection via the {@code connectionConfig}. In addition, it provides methods for
 * serializing and sending messages to RabbitMQ (with or without publish options).
 *
 * @param <T> Type of the elements in this sink
 */
public abstract class RabbitMQSinkWriterBase<T> implements SinkWriter<T, Void, RabbitMQSinkWriterState<T>> {
    protected static final Logger LOG = LoggerFactory.getLogger(RabbitMQSinkWriterBase.class);

    private final RabbitMQConnectionConfig connectionConfig;
    private final String queueName;
    private Connection rmqConnection;
    private Channel rmqChannel;
    private final SerializationSchema<T> serializationSchema;

    @Nullable private final RabbitMQSinkPublishOptions<T> publishOptions;

    @Nullable private final SerializableReturnListener returnListener;


    public RabbitMQSinkWriterBase(
            RabbitMQConnectionConfig connectionConfig,
            String queueName,
            SerializationSchema<T> serializationSchema,
            RabbitMQSinkPublishOptions<T> publishOptions,
            SerializableReturnListener returnListener) {
        this.connectionConfig = requireNonNull(connectionConfig);
        this.queueName = requireNonNull(queueName);
        this.serializationSchema = requireNonNull(serializationSchema);

        this.returnListener = returnListener;
        this.publishOptions = publishOptions;
    }

    /**
     * Receive the next stream element and publish it to RabbitMQ.
     *
     * @param element element from upstream flink task
     * @param context context of this sink writer
     */
    @Override
    public void write(T element, Context context) throws IOException {
        send(new RabbitMQSinkMessageWrapper<>(element, serializationSchema.serialize(element)));
    }

    /**
     * Recover the writer with a specific state.
     *
     * @param states a list of states to recover the reader with
     * @throws IOException that can be thrown as specialized writers might want to send messages.
     */
    public void recoverFromStates(List<RabbitMQSinkWriterState<T>> states) throws IOException {}

    /**
     * Setup the RabbitMQ connection and a channel to send messages to. In the end specialized
     * writers can configure the channel through {@link #configureChannel()}.
     *
     * @throws Exception that might occur when setting up the connection and channel.
     */
    public void setupRabbitMQ() throws Exception {
        this.rmqConnection = setupConnection(connectionConfig);
        this.rmqChannel = setupChannel(rmqConnection, queueName, returnListener);
        configureChannel();
    }

    private Connection setupConnection(RabbitMQConnectionConfig connectionConfig) throws Exception {
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
     * This method provides a hook in the constructor to apply additional configuration to the
     * channel. It is called at the end of the constructor.
     *
     * @throws IOException possible IOException that might be thrown when configuring the channel
     */
    protected void configureChannel() throws IOException {}

    /**
     * Only used by at-least-once and exactly-once for resending messages that could not be
     * delivered.
     *
     * @param message sink message wrapping the atomic message object
     */
    protected void send(RabbitMQSinkMessageWrapper<T> message) throws IOException {
        send(message.getMessage(), message.getBytes());
    }

    /**
     * Publish a message to a queue in RabbitMQ. With publish options enabled, first compute the
     * necessary publishing information.
     *
     * @param message original message, only required for publishing with publish options present
     * @param serializedMessage serialized message to send to RabbitMQ
     */
    protected void send(T message, byte[] serializedMessage) throws IOException {
        if (publishOptions == null) {
            rmqChannel.basicPublish("", queueName, null, serializedMessage);
        } else {
            publishWithOptions(message, serializedMessage);
        }
    }

    private void publishWithOptions(T message, byte[] serializedMessage) throws IOException {
        requireNonNull(publishOptions, "Try to publish with options without publishOptions.");

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

    @Override
    public List<Void> prepareCommit(boolean flush) {
        return Collections.emptyList();
    }

    @Override
    public List<RabbitMQSinkWriterState<T>> snapshotState() {
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
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

    protected SerializationSchema<T> getSerializationSchema() {
        return serializationSchema;
    }
}
