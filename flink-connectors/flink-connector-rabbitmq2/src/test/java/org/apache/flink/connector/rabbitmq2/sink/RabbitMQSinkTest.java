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

package org.apache.flink.connector.rabbitmq2.sink;

import org.apache.flink.connector.rabbitmq2.common.ConsistencyMode;
import org.apache.flink.connector.rabbitmq2.common.GlobalBoolean;
import org.apache.flink.connector.rabbitmq2.common.RabbitMQBaseTest;
import org.apache.flink.connector.rabbitmq2.common.RabbitMQContainerClient;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The tests for the rabbitmq sink with different consistency modes. As the tests are working a lot
 * with timeouts to uphold stream it is possible that tests might fail.
 */
public class RabbitMQSinkTest extends RabbitMQBaseTest {
    @Test
    public void atMostOnceTest() throws Exception {
        List<String> messages = getRandomMessages(100);

        DataStream<String> stream = env.fromCollection(messages);
        RabbitMQContainerClient client =
                addSinkOn(stream, ConsistencyMode.AT_MOST_ONCE, messages.size());
        env.execute();
        client.await();

        List<String> receivedMessages = client.getConsumedMessages();
        assertEquals(messages, receivedMessages);
    }

    @Test
    public void atLeastOnceTest() throws Exception {
        List<String> messages = getRandomMessages(100);
        DataStream<String> stream = env.fromCollection(messages);
        RabbitMQContainerClient client =
                addSinkOn(stream, ConsistencyMode.AT_LEAST_ONCE, messages.size());

        env.execute();
        client.await();

        List<String> receivedMessages = client.getConsumedMessages();
        assertEquals(messages, receivedMessages);
    }

    @Test
    public void atLeastOnceWithFlinkFailureTest() throws Exception {
        List<String> messages = getRandomMessages(100);

        GlobalBoolean shouldFail = new GlobalBoolean(true);
        DataStream<String> stream =
                env.fromCollection(messages)
                        .map(
                                m -> {
                                    if (m.equals("3") && shouldFail.get()) {
                                        shouldFail.set(false);
                                        throw new Exception("Supposed to be thrown.");
                                    }
                                    return m;
                                });
        RabbitMQContainerClient client =
                addSinkOn(stream, ConsistencyMode.AT_LEAST_ONCE, messages.size());

        env.execute();
        client.await();

        List<String> receivedMessages = client.getConsumedMessages();
        assertTrue(receivedMessages.containsAll(messages));
    }

    @Test
    public void exactlyOnceTest() throws Exception {
        List<String> messages = getRandomMessages(10);

        env.enableCheckpointing(100);
        DataStream<String> stream =
                env.fromCollection(messages)
                        .map(
                                m -> {
                                    TimeUnit.MILLISECONDS.sleep(100);
                                    return m;
                                });
        RabbitMQContainerClient client =
                addSinkOn(stream, ConsistencyMode.EXACTLY_ONCE, messages.size());

        env.execute();
        client.await();

        List<String> receivedMessages = client.getConsumedMessages();
        assertEquals(messages, receivedMessages);
    }
}
