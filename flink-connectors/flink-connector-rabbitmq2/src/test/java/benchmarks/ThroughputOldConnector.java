package benchmarks;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.rabbitmq2.ConsistencyMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** TODO. */
public class ThroughputOldConnector {

    String queue = "pub";
    ConsistencyMode mode = ConsistencyMode.AT_MOST_ONCE;
    int n = 5000000;
    String outputName = "benchmarksEC2_OldConnector/atleastThroughputBenchmark";

    public void sendToRabbit(int n, String queue)
            throws IOException, TimeoutException, InterruptedException {
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
        rmqChannel.queueDeclare(queue, true, false, false, null);

        System.out.println("Start Sending");

        for (int i = 0; i < n; i++) {
            byte[] message = ("Message: " + i).getBytes();
            if (i % 100000 == 0) {
                System.out.println("Send Message: " + i);
            }
            AMQP.BasicProperties props =
                    new AMQP.BasicProperties.Builder()
                            .correlationId(UUID.randomUUID().toString())
                            .build();
            rmqChannel.basicPublish("", queue, props, message);
        }

        System.out.println("Close Connection");

        rmqChannel.close();
        connection.close();

        TimeUnit.SECONDS.sleep(2);
    }

    @Test
    public void simpleAtMostOnceTest() throws Exception {
        sendToRabbit(n, queue);

        System.out.println("Start Flink");
        final RMQConnectionConfig connectionConfig =
                new RMQConnectionConfig.Builder()
                        .setHost("localhost")
                        .setVirtualHost("/")
                        .setUserName("guest")
                        .setPassword("guest")
                        .setPort(5672)
                        .build();

        RMQSource<String> rabbitMQSource =
                new RMQSource<>(connectionConfig, queue, new SimpleStringSchema());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        ExecutionConfig executionConfig = env.getConfig();
        executionConfig.enableObjectReuse();

        final DataStream<String> stream =
                env.addSource(rabbitMQSource, "RabbitMQSource").setParallelism(1);

        stream.map(message -> System.currentTimeMillis()).setParallelism(5).writeAsText(outputName);

        System.out.println("Start ENV");
        env.execute();
    }
}
