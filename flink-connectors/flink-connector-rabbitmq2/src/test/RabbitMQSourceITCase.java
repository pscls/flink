package org.apache.flink.tests.util.rabbitmq;

//import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
//
//import org.apache.flink.tests.util.flink.FlinkContainer;

import org.apache.flink.tests.util.flink.FlinkContainer;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitMQSourceITCase {

    public static final String INTER_CONTAINER_RABBITMQ_ALIAS = "rabbitmq";
    public final Network network;

    public final RabbitMQContainer rabbitMQContainer;

    private final RabbitMQContainerClient rabbitmqClient;

    public RabbitMQSourceITCase() throws IOException, TimeoutException {
        System.out.println("1");
//        this.network = Network.newNetwork();
        System.out.println("2");
        this.rabbitMQContainer = new RabbitMQContainer(DockerImageName.parse("rabbitmq"))

    System.out.println("3");

        this.rabbitmqClient = new RabbitMQContainerClient(this.rabbitmq);
        System.out.println("4");
    }

//    @Before
//    public void setUp() {
//        registryClient = new CachedSchemaRegistryClient(registry.getSchemaRegistryUrl(), 10);
//    }

    @Test(timeout = 120_000)
    public void testReading() throws Exception {
        System.out.println("Start Reading Test");
        rabbitmqClient.createQueue("FIND_A_GOOD_QUEUE_NAME_TBD");
        assertTrue(true);
    }
}
