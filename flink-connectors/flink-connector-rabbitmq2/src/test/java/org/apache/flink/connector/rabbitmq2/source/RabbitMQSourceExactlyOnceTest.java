package org.apache.flink.connector.rabbitmq2.source;

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

/** TODO. */
public class RabbitMQSourceExactlyOnceTest extends RabbitMQBaseTest {
    @Override
    public ConsistencyMode getConsistencyMode() {
        return ConsistencyMode.EXACTLY_ONCE;
    }

    @Test
    public void simpleFilterCorrelationIdsTest() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);

        DataStream<String> stream = getSinkOn(env);
        addCollectorSink(stream);
        env.executeAsync("RabbitMQ");

        List<String> correlationIds = Arrays.asList("1", "2", "3", "3", "3");
        List<String> messages = getSequentialMessages(5);
        sendToRabbit(messages, correlationIds);

        List<String> collectedMessages = getCollectedSinkMessages();
        List<String> expectedMessages = messages.subList(0, 3);
        assertEquals(expectedMessages, collectedMessages);
    }

    static boolean shouldFail = true;

    @Test
    public void exactlyOnceWithFailureTest() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(100);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 1000));

        DataStream<String> stream = getSinkOn(env);

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
