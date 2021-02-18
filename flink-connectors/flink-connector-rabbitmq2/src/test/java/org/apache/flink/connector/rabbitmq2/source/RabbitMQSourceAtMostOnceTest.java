package org.apache.flink.connector.rabbitmq2.source;

import org.apache.flink.connector.rabbitmq2.ConsistencyMode;
import org.apache.flink.connector.rabbitmq2.source.common.RabbitMQBaseTest;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/** TODO. */
public class RabbitMQSourceAtMostOnceTest extends RabbitMQBaseTest {

    @Override
    public ConsistencyMode getConsistencyMode() {
        return ConsistencyMode.AT_MOST_ONCE;
    }

    @Test
    public void simpleAtMostOnceTest() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> stream = getSinkOn(env);
        addCollectorSink(stream);
        env.executeAsync("RabbitMQ");

        List<String> messages = getRandomMessages(5);
        sendToRabbit(messages);

        assertEquals(
                CollectionUtils.getCardinalityMap(getCollectedSinkMessages()),
                CollectionUtils.getCardinalityMap(messages));
    }

    //    @Test
    //    public void simpleAtMostOnceTestWithException() throws Exception {
    //        final StreamExecutionEnvironment env =
    // StreamExecutionEnvironment.getExecutionEnvironment();
    //        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 1000));
    //
    //        DataStream<String> stream = getSinkOn(env);
    //
    //        List<String> messages = getSequentialMessages(5);
    //        System.out.println(messages);
    //        DataStream<String> outstream = stream.map((MapFunction<String, String>) message -> {
    //            System.out.println(message);
    //            if (message.equals("Message 3")) throw new Exception();
    //            return message;
    //        }).setParallelism(1);
    //        outstream.addSink(new CollectSink());
    //
    //        env.executeAsync("RabbitMQ");
    //
    //        sendToRabbit(messages);
    //        System.out.println(CollectSink.values);
    //        assertFalse(CollectSink.values.contains("Message 3"));
    //    }

}
