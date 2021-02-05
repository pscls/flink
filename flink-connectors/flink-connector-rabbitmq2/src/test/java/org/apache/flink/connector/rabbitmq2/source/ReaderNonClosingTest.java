package org.apache.flink.connector.rabbitmq2.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rabbitmq2.ConsistencyMode;
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

public class ReaderNonClosingTest {

    private static final int RABBITMQ_PORT = 5672;
    private RabbitMQContainerClient client;
    /**
     * RabbitMQ
     */
    @ClassRule
    public static RabbitMQContainer rabbitMq = new RabbitMQContainer(DockerImageName
            .parse("rabbitmq").withTag("3.7.25-management-alpine"))
            .withExposedPorts(RABBITMQ_PORT);

    @Before
    public void SetUpContainerClient() throws IOException, TimeoutException {
        client = new RabbitMQContainerClient(rabbitMq, false);

    }
    public DataStream<String> getStreamOn(StreamExecutionEnvironment env, String queueName) {
        final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost(rabbitMq.getHost())
                .setVirtualHost("/")
                .setUserName(rabbitMq.getAdminUsername())
                .setPassword(rabbitMq.getAdminPassword())
                .setPort(rabbitMq.getMappedPort(RABBITMQ_PORT))
                .build();

        RabbitMQSource<String> rabbitMQSource = new RabbitMQSource<>(
                connectionConfig,
                queueName,
                new SimpleStringSchema(),
                ConsistencyMode.AT_MOST_ONCE
        );

        final DataStream<String> stream = env
                .fromSource(rabbitMQSource,
                        WatermarkStrategy.noWatermarks(),
                        "RabbitMQSource")
                .setParallelism(1);

        return stream;
    }

    public void sendToRabbit(List<String> strings) throws IOException, InterruptedException {
        for (String message : strings) {
            client.sendMessages(new SimpleStringSchema(), message);
        }

        TimeUnit.SECONDS.sleep(3);
    }

    @Test
    public void simpleAtMostOnceTest() throws Exception {
        String queueName = "Test1";
        client.createQueue(queueName);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 1000));

        DataStream<String> stream = getStreamOn(env, queueName);

        stream.map((MapFunction<String, String>) message -> {
            System.out.println(message);
            if (message.equals("Message3")) throw new Exception();
            return message;
        }).setParallelism(1);

        env.executeAsync("RabbitMQ");

        for (int i = 0; i < 100; i++) {
            client.sendMessages(new SimpleStringSchema(), "Message"+i);
            TimeUnit.SECONDS.sleep(1);
        }


    }

}
