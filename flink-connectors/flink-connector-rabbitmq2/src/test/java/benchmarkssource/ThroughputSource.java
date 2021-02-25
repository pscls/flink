package benchmarkssource;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.rabbitmq2.ConsistencyMode;
import org.apache.flink.connector.rabbitmq2.source.RabbitMQSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** TODO. */
public class ThroughputSource {

    String queue = "pub";
    ConsistencyMode mode = ConsistencyMode.AT_MOST_ONCE;
    int n = 1000000;
    String outputName = "benchmarksEC2_final2/atmost_T_usable";

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
            byte[] message = String.valueOf(System.currentTimeMillis()).getBytes();
            if (i % 500000 == 0) {
                System.out.println("Send Message: " + i);
            }
            AMQP.BasicProperties props =
                    new AMQP.BasicProperties.Builder().correlationId("id_" + i).build();
            rmqChannel.basicPublish("", queue, null, message);
        }

        System.out.println("Close Connection");

        rmqChannel.close();
        connection.close();

        TimeUnit.SECONDS.sleep(4);
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

        RabbitMQSource<String> rabbitMQSource =
                RabbitMQSource.<String>builder()
                        .setConnectionConfig(connectionConfig)
                        .setQueueName(queue)
                        .setConsistencyMode(mode)
                        .setDeliveryDeserializer(new SimpleStringSchema())
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.enableCheckpointing(30000);
        ExecutionConfig executionConfig = env.getConfig();
        executionConfig.enableObjectReuse();

        final DataStream<String> stream =
                env.fromSource(rabbitMQSource, WatermarkStrategy.noWatermarks(), "RabbitMQSource")
                        .setParallelism(1);

        stream.map(message -> String.valueOf(System.currentTimeMillis()))
                .setParallelism(5)
                .writeAsText(outputName);

        System.out.println("Start ENV");
        env.execute();
        // TimeUnit.SECONDS.sleep(36);
        // sendToRabbit(n, queue);
    }
}
