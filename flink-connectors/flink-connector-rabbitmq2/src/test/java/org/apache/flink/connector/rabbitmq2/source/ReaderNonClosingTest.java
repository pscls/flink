package org.apache.flink.connector.rabbitmq2.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rabbitmq2.ConsistencyMode;
import org.apache.flink.connector.rabbitmq2.source.common.RabbitMQBaseTest;
import org.apache.flink.connector.rabbitmq2.source.common.RabbitMQContainerClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class ReaderNonClosingTest extends RabbitMQBaseTest {

    @Test
    public void simpleAtMostOnceTest() throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 1000));

        DataStream<String> stream = getSinkOn(env);

        stream.map((MapFunction<String, String>) message -> {
            System.out.println(message);
            if (message.equals("Message3")) throw new Exception();
            TimeUnit.SECONDS.sleep(2);
            return message;
        }).setParallelism(1);

        env.executeAsync("RabbitMQ");

        for (int i = 0; i < 100; i++) {
            client.sendMessages(new SimpleStringSchema(), "Message"+i);
            TimeUnit.SECONDS.sleep(1);
        }


    }

    @Override
    public ConsistencyMode getConsistencyMode() {
        return null;
    }
}
