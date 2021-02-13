package org.apache.flink.connector.rabbitmq2.source;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rabbitmq2.ConsistencyMode;
import org.apache.flink.connector.rabbitmq2.source.common.RabbitMQBaseTest;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.CloseableIterator;

import org.apache.flink.util.CollectionUtil;

import org.apache.flink.util.Collector;

import org.junit.Test;

import javax.sound.midi.SysexMessage;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class AtMostOnceReader extends RabbitMQBaseTest {

    @Override
    public ConsistencyMode getConsistencyMode() {
        return ConsistencyMode.AT_MOST_ONCE;
    }

    public static class MessageCollector extends ProcessFunction<String, String> {

        public final List<String> receivedMessages = new ArrayList<>();

        public void print() {
            System.out.println(receivedMessages);
        }

        private void checkMessage(String message) {
            receivedMessages.add(message);
//            print();
        }

        @Override
        public void processElement(
                String value,
                Context ctx,
                Collector<String> out) throws Exception {
            checkMessage(value);
            out.collect(value);
        }
    }

    @Test
    public void simpleAtMostOnceTest() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStream<String> stream = getSinkOn(env);

        List<String> messages = getRandomMessages(5);

        List<String> receivedMessages = new ArrayList<>();
        MessageCollector c = new MessageCollector();
        stream.process(c);
//        stream.map((MapFunction<String, String>) message -> {
//            System.out.println(message);
//            receivedMessages.add(message);
//            return message;
//        }).setParallelism(1);

        env.executeAsync("RabbitMQ");

        sendToRabbit(messages);
        c.receivedMessages
    }


}
