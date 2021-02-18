package org.apache.flink.connector.rabbitmq2.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.rabbitmq2.ConsistencyMode;
import org.apache.flink.connector.rabbitmq2.source.common.RabbitMQBaseTest;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** TODO. */
public class RabbitMQSourceAtLeastOnceTest extends RabbitMQBaseTest {

    @Override
    public ConsistencyMode getConsistencyMode() {
        return ConsistencyMode.AT_LEAST_ONCE;
    }

    @Test
    public void simpleAtLeastOnceTest() throws Exception {
        DataStream<String> stream = getSinkOn(env);
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
        DataStream<String> stream = getSinkOn(env);

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
}
