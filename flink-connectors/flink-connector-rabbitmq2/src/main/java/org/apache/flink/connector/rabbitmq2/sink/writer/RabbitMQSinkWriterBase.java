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
import org.apache.flink.connector.rabbitmq2.sink.common.RabbitMQSinkConnection;
import org.apache.flink.connector.rabbitmq2.sink.common.RabbitMQSinkMessageWrapper;
import org.apache.flink.connector.rabbitmq2.sink.common.RabbitMQSinkPublishOptions;
import org.apache.flink.connector.rabbitmq2.sink.common.SerializableReturnListener;
import org.apache.flink.connector.rabbitmq2.sink.state.RabbitMQSinkWriterState;
import org.apache.flink.connector.rabbitmq2.sink.writer.specialized.RabbitMQSinkWriterAtLeastOnce;
import org.apache.flink.connector.rabbitmq2.sink.writer.specialized.RabbitMQSinkWriterAtMostOnce;
import org.apache.flink.connector.rabbitmq2.sink.writer.specialized.RabbitMQSinkWriterExactlyOnce;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * RabbitMQSinkWriterBase is the common abstract class of {@link RabbitMQSinkWriterAtMostOnce},
 * {@link RabbitMQSinkWriterAtLeastOnce} and {@link RabbitMQSinkWriterExactlyOnce}
 *
 * <p>It provides basic functionality and common behaviour such as establishing and closing a
 * connection via the {@code connectionConfig} and methods for serializing and sending messages to
 * RabbitMQ (with or without publish options).
 *
 * @param <T> Type of the elements in this sink
 */
public abstract class RabbitMQSinkWriterBase<T> extends RabbitMQSinkConnection<T>
        implements SinkWriter<T, RabbitMQSinkWriterState<T>, RabbitMQSinkWriterState<T>> {
    protected final SerializationSchema<T> serializationSchema;

    public RabbitMQSinkWriterBase(
            RabbitMQConnectionConfig connectionConfig,
            String queueName,
            SerializationSchema<T> serializationSchema,
            RabbitMQSinkPublishOptions<T> publishOptions,
            SerializableReturnListener returnListener)
            throws Exception {
        super(connectionConfig, queueName, publishOptions, returnListener);
        this.serializationSchema = serializationSchema;
    }

    /**
     * Receive the next stream element and publish it to RabbitMQ.
     *
     * @param element element from upstream flink task
     * @param context context of this sink writer
     */
    @Override
    public void write(T element, Context context) {
        send(new RabbitMQSinkMessageWrapper<>(element, serializationSchema.serialize(element)));
    }

    @Override
    public List<RabbitMQSinkWriterState<T>> prepareCommit(boolean flush) {
        return Collections.emptyList();
    }

    @Override
    public List<RabbitMQSinkWriterState<T>> snapshotState() {
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
