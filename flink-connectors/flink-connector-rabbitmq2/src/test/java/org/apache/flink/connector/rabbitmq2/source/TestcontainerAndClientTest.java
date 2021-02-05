package org.apache.flink.connector.rabbitmq2.source;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.rabbitmq2.source.common.RabbitMQContainerClient;

import org.junit.ClassRule;
import org.junit.Test;
import static org.junit.Assert.*;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.sql.Time;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.rnorth.visibleassertions.VisibleAssertions.assertTrue;

/**
 * Tests for GenericContainerRules
 */
public class TestcontainerAndClientTest {

    private static final String RABBIQMQ_TEST_EXCHANGE = "TestExchange";
    private static final String RABBITMQ_TEST_ROUTING_KEY = "TestRoutingKey";
    private static final String RABBITMQ_TEST_MESSAGE = "Hello world";
    private static final int RABBITMQ_PORT = 5672;

    /**
     * RabbitMQ
     */
    @ClassRule
    public static RabbitMQContainer rabbitMq = new RabbitMQContainer(DockerImageName.parse("rabbitmq").withTag("3.7.25-management-alpine"))
            .withExposedPorts(RABBITMQ_PORT);

    @Test
    public void simpleContainerClientSendReceiveTest() throws IOException, TimeoutException, InterruptedException {
        RabbitMQContainerClient client = new RabbitMQContainerClient(rabbitMq);
        client.createQueue("Test");

        client.sendMessages(new SimpleStringSchema(), "test message");
        TimeUnit.SECONDS.sleep(2);
        List<String> messages = client.readMessages(new SimpleStringSchema());
        assertEquals(messages.size(), 1);
    }

    @Test
    public void simpleRabbitMqTest() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rabbitMq.getHost());
        factory.setPort(rabbitMq.getMappedPort(RABBITMQ_PORT));
        Connection connection = factory.newConnection();

        Channel channel = connection.createChannel();
        channel.exchangeDeclare(RABBIQMQ_TEST_EXCHANGE, "direct", true);
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, RABBIQMQ_TEST_EXCHANGE, RABBITMQ_TEST_ROUTING_KEY);

        // Set up a consumer on the queue
        final boolean[] messageWasReceived = new boolean[1];
        channel.basicConsume(queueName, false, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                messageWasReceived[0] = Arrays.equals(body, RABBITMQ_TEST_MESSAGE.getBytes());
            }
        });

        // post a message
        channel.basicPublish(RABBIQMQ_TEST_EXCHANGE, RABBITMQ_TEST_ROUTING_KEY, null, RABBITMQ_TEST_MESSAGE.getBytes());

        // check the message was received
        assertTrue("The message was received", Unreliables.retryUntilSuccess(5, TimeUnit.SECONDS, () -> {
            if (!messageWasReceived[0]) {
                throw new IllegalStateException("Message not received yet");
            }
            return true;
        }));
    }
}
