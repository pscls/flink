package org.apache.flink.connector.rabbitmq2.source.common;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.rabbitmq2.ConsistencyMode;
import org.apache.flink.connector.rabbitmq2.source.RabbitMQSource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.UUID;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class RabbitMQBaseTest {

    // TODO: Find out how to run multiple tests in the same class
    // TODO: Find out how to run multiple tests from different classes -> stop container??

    protected static final int RABBITMQ_PORT = 5672;
    protected RabbitMQContainerClient client;
    protected String queueName;

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(1)
                            .setNumberTaskManagers(1)
                            .build());

    protected final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 1000));
    }

    @AfterClass
    public static void after() {
        rabbitMq.stop();
        rabbitMq.close();
    }

    public abstract ConsistencyMode getConsistencyMode();

    public DataStream<String> getSinkOn(StreamExecutionEnvironment env) throws IOException, TimeoutException {
        queueName = UUID.randomUUID().toString();
        client.createQueue(queueName);
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
                getConsistencyMode()
        );

        final DataStream<String> stream = env
                .fromSource(rabbitMQSource,
                        WatermarkStrategy.noWatermarks(),
                        "RabbitMQSource")
                .setParallelism(1);

        return stream;
    }

    public void sendToRabbit(List<String> messages) throws IOException, InterruptedException {
        for (String message : messages) {
            client.sendMessages(new SimpleStringSchema(), message);
        }

        TimeUnit.SECONDS.sleep(5);
    }

    public void sendToRabbit(List<String> messages, List<String> correlationIds) throws IOException, InterruptedException {
        for (int i = 0; i < messages.size(); i++) {
            TimeUnit.SECONDS.sleep(1);
            client.sendMessages(new SimpleStringSchema(), messages.get(i), correlationIds.get(i));
        }

        TimeUnit.SECONDS.sleep(5);
    }

    public void sendToRabbit(int numberOfMessage) throws IOException, InterruptedException {
        sendToRabbit(getRandomMessages(numberOfMessage));
    }

    public List<String> getRandomMessages(int numberOfMessages) {
        List<String> messages = new ArrayList<>();
        for (int i = 0; i < numberOfMessages; i++) {
            messages.add(UUID.randomUUID().toString());
        }
        return messages;
    }

    public List<String> getSequentialMessages(int numberOfMessages) {
        List<String> messages = new ArrayList<>();
        for (int i = 0; i < numberOfMessages; i++) {
            messages.add("Message " + i);
        }
        return messages;
    }

    public List<String> getCollectedSinkMessages() {
        List<String> messages = new ArrayList<>(CollectSink.values);
        CollectSink.values.clear();
        return messages;
    }

    public void addCollectorSink(DataStream<String> stream) {
        stream.addSink(new CollectSink());
    }

    public static class CollectSink implements SinkFunction<String> {

        // must be static
        public static final List<String> values = Collections.synchronizedList(new ArrayList<>());

        public CollectSink() {
            super();
            System.out.println("Sink created");
        }

        @Override
        public void invoke(String value) throws Exception {
            values.add(value);
        }
    }
}
