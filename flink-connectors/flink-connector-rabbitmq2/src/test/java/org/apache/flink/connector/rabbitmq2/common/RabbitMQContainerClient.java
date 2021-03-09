package org.apache.flink.connector.rabbitmq2.common;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeoutException;

/** TODO. */
public class RabbitMQContainerClient {

    private final RabbitMQContainer container;
    private Channel channel;
    private final Queue<byte[]> messages;
    private String queueName;

    public RabbitMQContainerClient(RabbitMQContainer container, boolean withConsumer)
            throws IOException, TimeoutException {
        container.withExposedPorts(5762).waitingFor(Wait.forListeningPort());
        this.container = container;
        this.messages = new LinkedList<>();
    }

    public void createQueue(String queueName, Boolean withConsumer)
            throws IOException, TimeoutException {
        this.queueName = queueName;
        Connection connection = getRabbitMQConnection();
        this.channel = connection.createChannel();
        channel.queueDeclare(queueName, true, false, false, null);
        if (withConsumer) {
            messages.clear();
            final DeliverCallback deliverCallback = this::handleMessageReceivedCallback;
            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});
        }
    }

    public void createQueue(String queueName) throws IOException, TimeoutException {
        createQueue(queueName, false);
    }

    public <T> void sendMessages(SerializationSchema<T> valueSerializer, T... messages)
            throws IOException {
        for (T message : messages) {
            channel.basicPublish("", queueName, null, valueSerializer.serialize(message));
        }
    }

    public <T> void sendMessages(
            SerializationSchema<T> valueSerializer, T message, String correlationId)
            throws IOException {
        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
        builder.correlationId(correlationId);
        AMQP.BasicProperties properties = builder.build();
        channel.basicPublish("", queueName, properties, valueSerializer.serialize(message));
    }

    public <T> List<T> readMessages(DeserializationSchema<T> valueDeserializer) throws IOException {
        List<T> deserializedMessages = new ArrayList<>();
        while (!messages.isEmpty()) {
            T message = valueDeserializer.deserialize(messages.poll());
            deserializedMessages.add(message);
        }
        return deserializedMessages;
    }

    protected void handleMessageReceivedCallback(String consumerTag, Delivery delivery)
            throws IOException {
        byte[] body = delivery.getBody();
        messages.add(body);
    }

    private Connection getRabbitMQConnection() throws TimeoutException, IOException {
        ConnectionFactory factory = new ConnectionFactory();

        factory.setUsername(container.getAdminUsername());
        factory.setPassword(container.getAdminPassword());
        factory.setVirtualHost("/");
        factory.setHost(container.getHost());
        factory.setPort(container.getAmqpPort());

        return factory.newConnection();
    }
}
