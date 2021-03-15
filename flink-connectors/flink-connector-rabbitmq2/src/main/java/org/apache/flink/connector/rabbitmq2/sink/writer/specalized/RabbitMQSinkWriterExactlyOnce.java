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

package org.apache.flink.connector.rabbitmq2.sink.writer.specalized;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.rabbitmq2.sink.RabbitMQSink;
import org.apache.flink.connector.rabbitmq2.sink.common.RabbitMQSinkMessageWrapper;
import org.apache.flink.connector.rabbitmq2.sink.state.RabbitMQSinkWriterState;

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
public class RabbitMQSinkWriterExactlyOnce<T>
        implements SinkWriter<T, RabbitMQSinkWriterState<T>, RabbitMQSinkWriterState<T>> {

    private final SerializationSchema<T> serializationSchema;
    /** All messages that arrived and could not be committed this far. */
    private List<RabbitMQSinkMessageWrapper<T>> messages;

    /**
     * Create a new RabbitMQSinkWriterExactlyOnce.
     *
     * @param serializationSchema serialization schema to turn elements into byte representation
     * @param states a list of states to initialize this reader with
     */
    public RabbitMQSinkWriterExactlyOnce(
            SerializationSchema<T> serializationSchema, List<RabbitMQSinkWriterState<T>> states) {
        messages = new ArrayList<>();
        this.serializationSchema = serializationSchema;
        initWithState(states);
    }

    private void initWithState(List<RabbitMQSinkWriterState<T>> states) {
        List<RabbitMQSinkMessageWrapper<T>> messages = new ArrayList<>();
        for (RabbitMQSinkWriterState<T> state : states) {
            messages.addAll(state.getOutstandingMessages());
        }
        this.messages = messages;
    }

    @Override
    public void write(T element, Context context) {
        messages.add(
                new RabbitMQSinkMessageWrapper<>(element, serializationSchema.serialize(element)));
    }

    @Override
    public List<RabbitMQSinkWriterState<T>> prepareCommit(boolean flush) {
        List<RabbitMQSinkMessageWrapper<T>> messagesToSend = new ArrayList<>(messages);
        messages.subList(0, messagesToSend.size()).clear();
        return Collections.singletonList(new RabbitMQSinkWriterState<>(messagesToSend));
    }

    @Override
    public List<RabbitMQSinkWriterState<T>> snapshotState() {
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {}
}
