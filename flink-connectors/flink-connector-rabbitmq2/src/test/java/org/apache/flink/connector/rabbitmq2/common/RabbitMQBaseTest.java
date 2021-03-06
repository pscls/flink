package org.apache.flink.connector.rabbitmq2.common;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.rabbitmq2.ConsistencyMode;
import org.apache.flink.connector.rabbitmq2.sink.RabbitMQSink;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.Before;
import org.junit.Rule;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** TODO. */
public abstract class RabbitMQBaseTest {

    // TODO: Find out how to run multiple tests in the same class
    // TODO: Find out how to run multiple tests from different classes -> stop container??

    protected static final int RABBITMQ_PORT = 5672;
    protected RabbitMQContainerClient client;
    protected String queueName;

    @Rule
    public MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(1)
                            .setNumberTaskManagers(1)
                            .build());

    protected StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    @Rule
    public RabbitMQContainer rabbitMq =
            new RabbitMQContainer(
                            DockerImageName.parse("rabbitmq").withTag("3.7.25-management-alpine"))
                    .withExposedPorts(RABBITMQ_PORT);

    @Before
    public void setUpContainerClient() throws IOException, TimeoutException {
        client = new RabbitMQContainerClient(rabbitMq, false);
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 1000));
    }

    public void addSinkOn(DataStream<String> stream, ConsistencyMode consistencyMode)
            throws IOException, TimeoutException {
        queueName = UUID.randomUUID().toString();
        client.createQueue(queueName, true);

        final RMQConnectionConfig connectionConfig =
                new RMQConnectionConfig.Builder()
                        .setHost(rabbitMq.getHost())
                        .setVirtualHost("/")
                        .setUserName(rabbitMq.getAdminUsername())
                        .setPassword(rabbitMq.getAdminPassword())
                        .setPort(rabbitMq.getMappedPort(RABBITMQ_PORT))
                        .build();

        RabbitMQSink<String> sink =
                RabbitMQSink.<String>builder()
                        .setConnectionConfig(connectionConfig)
                        .setQueueName(queueName)
                        .setSerializationSchema(new SimpleStringSchema())
                        .setConsistencyMode(consistencyMode)
                        .build();
        stream.sinkTo(sink).setParallelism(1);
    }

    public List<String> getMessageFromRabbit() throws IOException {
        return client.readMessages(new SimpleStringSchema());
    }

    public void sendToRabbit(List<String> messages) throws IOException, InterruptedException {
        for (String message : messages) {
            client.sendMessages(new SimpleStringSchema(), message);
        }

        TimeUnit.SECONDS.sleep(3);
    }

    public void sendToRabbit(List<String> messages, List<String> correlationIds)
            throws IOException, InterruptedException {
        sendToRabbit(messages, correlationIds, 100);
    }

    public void sendToRabbit(List<String> messages, List<String> correlationIds, int delay)
            throws IOException, InterruptedException {
        for (int i = 0; i < messages.size(); i++) {
            TimeUnit.MILLISECONDS.sleep(delay);
            client.sendMessages(new SimpleStringSchema(), messages.get(i), correlationIds.get(i));
        }

        TimeUnit.SECONDS.sleep(3);
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
        List<String> messages = new ArrayList<>(CollectSink.VALUES);
        CollectSink.VALUES.clear();
        return messages;
    }

    public void addCollectorSink(DataStream<String> stream) {
        stream.addSink(new CollectSink());
    }

    /** TODO. */
    public static class CollectSink implements SinkFunction<String> {

        // must be static
        public static final List<String> VALUES = Collections.synchronizedList(new ArrayList<>());

        public CollectSink() {
            super();
            System.out.println("Sink created");
        }

        @Override
        public void invoke(String value) throws Exception {
            VALUES.add(value);
        }
    }
}
