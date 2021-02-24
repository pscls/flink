package org.apache.flink.connector.rabbitmq2.source;

import org.apache.commons.collections.CollectionUtils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.rabbitmq2.ConsistencyMode;
import org.apache.flink.connector.rabbitmq2.source.common.RabbitMQBaseTest;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RabbitMQSourceTest extends RabbitMQBaseTest {

    // at-most-once
    @Test
    public void simpleAtMostOnceTest() throws Exception {
        DataStream<String> stream = getSinkOn(env, ConsistencyMode.AT_MOST_ONCE);
        addCollectorSink(stream);
        env.executeAsync("RabbitMQ");

        List<String> messages = getRandomMessages(5);
        sendToRabbit(messages);

        assertEquals(
                CollectionUtils.getCardinalityMap(getCollectedSinkMessages()),
                CollectionUtils.getCardinalityMap(messages));
    }

    // at-least-once
    @Test
    public void simpleAtLeastOnceTest() throws Exception {
        DataStream<String> stream = getSinkOn(env, ConsistencyMode.AT_LEAST_ONCE);
        addCollectorSink(stream);
        env.executeAsync("RabbitMQ");

        List<String> messages = getRandomMessages(5);
        sendToRabbit(messages);

        assertEquals(
                CollectionUtils.getCardinalityMap(messages),
                CollectionUtils.getCardinalityMap(getCollectedSinkMessages()));
    }

    static boolean shouldFail = true;

    @Test
    public void simpleAtLeastOnceFailureTest() throws Exception {
        DataStream<String> stream = getSinkOn(env, ConsistencyMode.AT_LEAST_ONCE);

        List<String> messages = getSequentialMessages(5);
        System.out.println(messages);

        shouldFail = true;

        DataStream<String> outstream =
                stream.map(
                        (MapFunction<String, String>)
                                message -> {
                                    System.out.println(message);
                                    if (message.equals("Message 2") && shouldFail) {
                                        shouldFail = false;
                                        throw new Exception();
                                    }
                                    return message;
                                })
                        .setParallelism(1);
        outstream.addSink(new CollectSink());

        env.executeAsync("RabbitMQ");

        sendToRabbit(messages);
        System.out.println(CollectSink.VALUES);
        List<String> collectedMessages = getCollectedSinkMessages();
        assertTrue(collectedMessages.containsAll(messages));
    }

    // exactly-once
    @Test
    public void simpleFilterCorrelationIdsTest() throws Exception {
        DataStream<String> stream = getSinkOn(env, ConsistencyMode.EXACTLY_ONCE);
        addCollectorSink(stream);
        env.executeAsync("RabbitMQ");

        List<String> correlationIds = Arrays.asList("1", "2", "3", "3", "3");
        List<String> messages = getSequentialMessages(5);
        sendToRabbit(messages, correlationIds);

        List<String> collectedMessages = getCollectedSinkMessages();
        List<String> expectedMessages = messages.subList(0, 3);
        assertEquals(expectedMessages, collectedMessages);
    }

    @Test
    public void exactlyOnceWithFailureTest() throws Exception {
        env.enableCheckpointing(100);
        DataStream<String> stream = getSinkOn(env, ConsistencyMode.EXACTLY_ONCE);

        addCollectorSink(stream);

        List<String> messages = getSequentialMessages(5);
        List<String> correlationIds = Arrays.asList("1", "2", "3", "4", "5");
        System.out.println(messages);

        shouldFail = true;

        DataStream<String> outstream =
                stream.map(
                        (MapFunction<String, String>)
                                message -> {
                                    System.out.println(message);
                                    if (message.equals("Message 2") && shouldFail) {
                                        shouldFail = false;
//                                        CollectSink.VALUES.clear();
                                        throw new Exception();
                                    }
                                    return message;
                                })
                        .setParallelism(1);
        outstream.addSink(new CollectSink());

        env.executeAsync("RabbitMQ");

        sendToRabbit(messages, correlationIds);
        System.out.println(CollectSink.VALUES);
        List<String> collectedMessages = getCollectedSinkMessages();
        assertEquals(messages, collectedMessages);
    }
}
