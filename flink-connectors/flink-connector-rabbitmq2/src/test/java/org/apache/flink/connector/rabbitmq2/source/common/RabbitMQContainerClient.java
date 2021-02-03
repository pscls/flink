package org.apache.flink.connector.rabbitmq2.source.common;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import org.testcontainers.containers.RabbitMQContainer;

import com.rabbitmq.client.Channel;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;

import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;


import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeoutException;


public class RabbitMQContainerClient {

    private final RabbitMQContainer container;
    private Channel channel;
    private final Queue<byte[]> messages;
    private String queueName;

    public RabbitMQContainerClient(RabbitMQContainer container) throws IOException, TimeoutException {
        container.withExposedPorts(5762).waitingFor(Wait.forListeningPort());
        this.container = container;
        this.messages = new LinkedList<>();
    }

    public void createQueue(String queueName) throws IOException, TimeoutException {
        this.queueName = queueName;
        Connection connection = getRabbitMQConnection();
        this.channel = connection.createChannel();
        channel.queueDeclare(queueName, true, false, false, null);
        final DeliverCallback deliverCallback = this::handleMessageReceivedCallback;
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});
    }

    public <T> void sendMessages(
            SerializationSchema<T> valueSerializer,
            T... messages)
            throws IOException {
        for (T message : messages) {
            channel.basicPublish("", queueName, null, valueSerializer.serialize(message));
        }
    }

    public <T> List<T> readMessages(
            int expectedNumMessages,
            DeserializationSchema<T> valueDeserializer)
            throws IOException {
        List<T> deserializedMessages = new ArrayList<>();
        for (int i = 0; i < Math.min(expectedNumMessages, messages.size()); i++) {
            T message = valueDeserializer.deserialize(messages.poll());
            deserializedMessages.add(message);
        }

        return deserializedMessages;
    }

    protected void handleMessageReceivedCallback(String consumerTag, Delivery delivery) throws IOException {
//        AMQP.BasicProperties properties = delivery.getProperties();

        byte[] body = delivery.getBody();
        System.out.println("Received message: " + body);
//        Envelope envelope = delivery.getEnvelope();
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
