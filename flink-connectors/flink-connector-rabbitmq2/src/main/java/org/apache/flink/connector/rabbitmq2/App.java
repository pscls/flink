package org.apache.flink.connector.rabbitmq2;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.connector.rabbitmq2.source.RabbitMQSourceBuilder;
import org.apache.flink.connector.rabbitmq2.source.common.AcknowledgeMode;
import org.apache.flink.connector.rabbitmq2.source.reader.RabbitMQSourceReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.connector.rabbitmq2.source.RabbitMQSource;
import org.apache.flink.configuration.Configuration;

public class App {
	public static void main(String[] args) throws Exception {
		System.out.println("Starting");

    	//final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		PropertyConfigurator.configure("log4j.properties");

		final Configuration conf = new Configuration();
    	final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
		// checkpointing is required for exactly-once or at-least-once guarantees

//		env.enableCheckpointing(5000);

		// ====================== Source ========================
		final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
			.setHost("localhost")
			.setVirtualHost("/")
			.setUserName("guest")
			.setPassword("guest")
			.setPort(5672)
			.setPrefetchCount(10)
    		.build();

//		final DataStream<String> stream = env
//			.addSource(new RMQSource<String>(
//				connectionConfig,
//				// config for the RabbitMQ connection
//				"pub",
//				// name of the RabbitMQ queue to consume
//				true,
//				// use correlation ids; can be false if only at-least-once is required
//				new SimpleStringSchema()))   // deserialization schema to turn messages into Java objects
////			.setParallelism(1);
//		RabbitMQSource<String> rabbitMQSource = RabbitMQSource.
//			<String>builder()
//			.build(
//				connectionConfig,
//				"pub",
//				new SimpleStringSchema(),
//				AcknowledgeMode.AUTO
//			);

		RabbitMQSource<String> rabbitMQSource = new RabbitMQSource<>(
			connectionConfig,
			"pub",
			new SimpleStringSchema(),
			AcknowledgeMode.CHECKPOINT
		);

		final DataStream<String> stream = env
			.fromSource(rabbitMQSource,
				WatermarkStrategy.noWatermarks(),
				"RabbitMQSource")
			.setParallelism(1);


		DataStream<String> mappedMessages = stream
			.map((MapFunction<String, String>) message -> {
//				System.out.println(message);
				return "Mapped: " + message;
			});

		// ====================== SINK ========================
		mappedMessages.addSink(new RMQSink<>(
			connectionConfig,            // config for the RabbitMQ connection
			"sub",                 // name of the RabbitMQ queue to send messages to
			new SimpleStringSchema()));  // serialization schema to turn Java objects to messages


		env.execute("RabbitMQ");
	}
}
