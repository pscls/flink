package benchmarkssource;

import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/** TODO. */
public class DataGeneratorSource {

    @Test
    public void generate() throws IOException, TimeoutException {
        System.out.println("Start Connection");
        final RMQConnectionConfig connectionConfig =
                new RMQConnectionConfig.Builder()
                        .setHost("localhost")
                        .setVirtualHost("/")
                        .setUserName("guest")
                        .setPassword("guest")
                        .setPort(5672)
                        .build();

        final ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(connectionConfig.getHost());
        Connection connection = connectionFactory.newConnection();

        final Channel rmqChannel = connection.createChannel();
        rmqChannel.queueDeclare("pub", true, false, false, null);

        System.out.println("Start Sending");

        for (int i = 0; i < 50000000; i++) {
            byte[] message = String.valueOf(System.currentTimeMillis()).getBytes();
            if (i % 1000000 == 0) {
                System.out.println("Send Message: " + i);
            }
            AMQP.BasicProperties props =
                    new AMQP.BasicProperties.Builder()
                            .correlationId(UUID.randomUUID().toString())
                            .build();
            rmqChannel.basicPublish("", "pub", null, message);
        }

        System.out.println("Close Connection");

        rmqChannel.close();
        connection.close();
    }
}
