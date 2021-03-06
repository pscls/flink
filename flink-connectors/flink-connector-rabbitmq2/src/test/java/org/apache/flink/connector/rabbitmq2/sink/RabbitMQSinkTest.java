package org.apache.flink.connector.rabbitmq2.sink;

import org.apache.flink.connector.rabbitmq2.ConsistencyMode;
import org.apache.flink.connector.rabbitmq2.common.RabbitMQBaseTest;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class RabbitMQSinkTest extends RabbitMQBaseTest {

    static Boolean shouldFail = true;

    @Test
    public void simpleAtMostOnceTest() throws Exception {
        List<String> messages = Arrays.asList("1", "2", "3", "4", "5");

        DataStream<String> stream = env.fromCollection(messages);
        addSinkOn(stream, ConsistencyMode.AT_MOST_ONCE);

        TimeUnit.SECONDS.sleep(3);

        List<String> receivedMessages = getMessageFromRabbit();

        assertEquals(messages, receivedMessages);
    }

    @Test
    public void simpleAtLeastOnceTest() throws Exception {
        List<String> messages = Arrays.asList("1", "2", "3", "4", "5");

        DataStream<String> stream = env.fromCollection(messages);
        addSinkOn(stream, ConsistencyMode.AT_LEAST_ONCE);
        env.execute();

        TimeUnit.SECONDS.sleep(3);

        List<String> receivedMessages = getMessageFromRabbit();

        assertEquals(messages, receivedMessages);
    }

    @Test
    public void simpleExactlyOnceTest() throws Exception {
        List<String> messages = Arrays.asList("1", "2", "3", "4", "5");
        env.enableCheckpointing(100);
        DataStream<String> stream = env.fromCollection(messages);
        shouldFail = true;
        DataStream<String> stream2 =
                stream.map(
                        m -> {
                            TimeUnit.SECONDS.sleep(1);
                            return m;
                        });
        addSinkOn(stream2, ConsistencyMode.EXACTLY_ONCE);

        env.execute();

        TimeUnit.SECONDS.sleep(3);
        List<String> receivedMessages = getMessageFromRabbit();
        assertEquals(messages, receivedMessages);
    }
}
