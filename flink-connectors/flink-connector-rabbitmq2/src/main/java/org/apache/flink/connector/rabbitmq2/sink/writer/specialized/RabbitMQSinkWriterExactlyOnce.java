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

package org.apache.flink.connector.rabbitmq2.sink.writer.specialized;

import com.rabbitmq.client.ConfirmCallback;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.rabbitmq2.common.RabbitMQConnectionConfig;
import org.apache.flink.connector.rabbitmq2.sink.RabbitMQSink;
import org.apache.flink.connector.rabbitmq2.sink.common.RabbitMQSinkMessageWrapper;
import org.apache.flink.connector.rabbitmq2.sink.common.RabbitMQSinkPublishOptions;
import org.apache.flink.connector.rabbitmq2.sink.common.SerializableReturnListener;
import org.apache.flink.connector.rabbitmq2.sink.state.RabbitMQSinkWriterState;
import org.apache.flink.connector.rabbitmq2.sink.writer.RabbitMQSinkWriterBase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A {@link SinkWriter} implementation for {@link RabbitMQSink} that provides exactly-once delivery
 * guarantees, which means incoming stream elements will be delivered to RabbitMQ exactly once. For
 * this, checkpointing needs to be enabled.
 *
 * <p>Exactly-once behaviour is implemented using a transactional RabbitMQ channel. All incoming
 * stream elements are buffered in the state of this writer until the next checkpoint is triggered.
 * All buffered {@code messages} are then send to RabbitMQ in a single transaction. When successful,
 * all messages committed get removed from the state. If the transaction is aborted, all messages
 * are put back into the state and send on the next checkpoint.
 *
 * <p>The transactional channel is heavyweight and will decrease throughput. If the system is under
 * heavy load, consecutive checkpoints can be delayed if commits take longer than the checkpointing
 * interval specified. Only use exactly-once if necessary (no duplicated messages in RabbitMQ
 * allowed), otherwise consider using at-least-once.
 *
 * @param <T> Type of the elements in this sink
 */
public class RabbitMQSinkWriterExactlyOnce<T> extends RabbitMQSinkWriterBase<T> {

    /** All messages that arrived and could not be committed this far. */
    private List<RabbitMQSinkMessageWrapper<T>> messages;

    /**
     * Create a new RabbitMQSinkWriterExactlyOnce.
     *
     * @param connectionConfig configuration parameters used to connect to RabbitMQ
     * @param queueName name of the queue to publish to
     * @param serializationSchema serialization schema to turn elements into byte representation
     * @param publishOptions optionally used to compute routing/exchange for messages
     * @param returnListener return listener
     * @param states a list of states to initialize this reader with
     */
    public RabbitMQSinkWriterExactlyOnce(
            RabbitMQConnectionConfig connectionConfig,
            String queueName,
            SerializationSchema<T> serializationSchema,
            RabbitMQSinkPublishOptions<T> publishOptions,
            SerializableReturnListener returnListener,
            List<RabbitMQSinkWriterState<T>> states) throws Exception {
        super(
                connectionConfig,
                queueName,
                serializationSchema,
                publishOptions,
                returnListener);
        messages = new ArrayList<>();
        initWithState(states);
        configureChannel();
    }

    private void initWithState(List<RabbitMQSinkWriterState<T>> states) {
        List<RabbitMQSinkMessageWrapper<T>> messages = new ArrayList<>();
        for (RabbitMQSinkWriterState<T> state : states) {
            messages.addAll(state.getOutstandingMessages());
        }
        this.messages = messages;
    }

    private void configureChannel() throws IOException {
        // puts channel in commit mode
        getRmqChannel().txSelect();
    }

    @Override
    public void write(T element, Context context) {
        messages.add(new RabbitMQSinkMessageWrapper<>(element, serializationSchema.serialize(element)));
    }

    @Override
    public List<RabbitMQSinkWriterState<T>> snapshotState() {
        commitMessages();
        return Collections.singletonList(new RabbitMQSinkWriterState<>(messages));
    }

    private void commitMessages() {
        List<RabbitMQSinkMessageWrapper<T>> messagesToSend = new ArrayList<>(messages);
        messages.subList(0, messagesToSend.size()).clear();
        try {
            for (RabbitMQSinkMessageWrapper<T> msg : messagesToSend) {
                super.send(msg);
            }
            getRmqChannel().txCommit();
            LOG.info("Successfully committed {} messages.", messagesToSend.size());
        } catch (IOException e) {
            LOG.error(
                    "Error during commit of {} messages. Rollback Messages. Error: {}",
                    messagesToSend.size(),
                    e.getMessage());
            messages.addAll(0, messagesToSend);
            try {
                getRmqChannel().txRollback();
            } catch (IOException rollbackException) {
                throw new RuntimeException(rollbackException.getMessage());
            }
        }
    }
}
