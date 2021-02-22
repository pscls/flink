package benchmarkssink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.rabbitmq2.ConsistencyMode;
import org.apache.flink.connector.rabbitmq2.sink.RabbitMQSink;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import org.junit.Test;

/** TODO. */
public class ThroughputSink {

    @Test
    public void main() throws Exception {
        System.out.println("Starting");

        String queue = "pub";
        ConsistencyMode mode = ConsistencyMode.AT_LEAST_ONCE;

        final Configuration conf = new Configuration();
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        env.enableCheckpointing(10000);

        //		env.enableCheckpointing(2000);

        // ====================== Source ========================
        final RMQConnectionConfig connectionConfig =
                new RMQConnectionConfig.Builder()
                        .setHost("localhost")
                        .setVirtualHost("/")
                        .setUserName("guest")
                        .setPassword("guest")
                        .setPort(5672)
                        .build();

        // Data generator source
        DataGenerator<String> generator =
                new DataGenerator<>() {
                    @Override
                    public void open(
                            String s,
                            FunctionInitializationContext functionInitializationContext,
                            RuntimeContext runtimeContext) {}

                    @Override
                    public boolean hasNext() {
                        return true;
                    }

                    @Override
                    public String next() {
                        return String.valueOf(System.currentTimeMillis());
                    }
                };
        DataGeneratorSource<String> source = new DataGeneratorSource<>(generator);
        SingleOutputStreamOperator<String> stream = env.addSource(source).returns(String.class);

        //        RMQSink<String> sink = new RMQSink<>(connectionConfig, "pub", new
        // SimpleStringSchema());
        //        stream.addSink(sink);

        RabbitMQSink<String> sink =
                RabbitMQSink.<String>builder()
                        .setConnectionConfig(connectionConfig)
                        .setQueueName(queue)
                        .setConsistencyMode(mode)
                        .setSerializationSchema(new SimpleStringSchema())
                        .build();

        stream.sinkTo(sink).setParallelism(1);

        env.execute("RabbitMQ");
    }
}
