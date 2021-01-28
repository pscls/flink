package org.apache.flink.connector.rabbitmq2;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.rabbitmq2.sink.RabbitMQSink;
import org.apache.flink.connector.rabbitmq2.source.common.ConsistencyMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.connector.rabbitmq2.source.RabbitMQSource;
import org.apache.flink.configuration.Configuration;

import org.apache.log4j.PropertyConfigurator;

public class App {
	public static void main(String[] args) throws Exception {
		System.out.println("Starting");

//    	final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		PropertyConfigurator.configure("log4j.properties");

		final Configuration conf = new Configuration();
    	final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
		// checkpointing is required for exactly-once or at-least-once guarantees



		// ====================== Source ========================
		final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
			.setHost("localhost")
			.setVirtualHost("/")
			.setUserName("guest")
			.setPassword("guest")
			.setPort(5672)
			.setPrefetchCount(1000)
    		.build();

		RabbitMQSource<String> rabbitMQSource = new RabbitMQSource<>(
			connectionConfig,
			"pub",
			new SimpleStringSchema(),
			ConsistencyMode.AT_LEAST_ONCE_AFTER_CHECKPOINTING
		);

		final DataStream<String> stream = env
			.fromSource(rabbitMQSource,
				WatermarkStrategy.noWatermarks(),
				"RabbitMQSource")
			.setParallelism(1);


		DataStream<String> mappedMessages = stream
			.map((MapFunction<String, String>) message -> {
//				System.out.println("Mapped" + message);
				return "Mapped: " + message;
			}).setParallelism(1);

		// ====================== SINK ========================
        RabbitMQSink<String> sink = new RabbitMQSink<>(
                connectionConfig,            // config for the RabbitMQ connection
                "sub",                 // name of the RabbitMQ queue to send messages to
                new SimpleStringSchema(), // serialization schema to turn Java objects to messages
                ConsistencyMode.AT_MOST_ONCE);

        mappedMessages.sinkTo(sink).setParallelism(1);

//		mappedMessages.addSink(new RMQSink<>(
//			connectionConfig,            // config for the RabbitMQ connection
//			"sub",                 // name of the RabbitMQ queue to send messages to
//			new SimpleStringSchema()));  // serialization schema to turn Java objects to messages

        env.enableCheckpointing(1000);

		env.execute("RabbitMQ");
	}
}
