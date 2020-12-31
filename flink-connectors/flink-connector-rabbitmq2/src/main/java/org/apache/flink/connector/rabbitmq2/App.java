package org.apache.flink.connector.rabbitmq2;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSink;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

public class App {
	public static void main(String[] args) throws Exception {
		System.out.println("Starting");

    	final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// checkpointing is required for exactly-once or at-least-once guarantees
//		env.enableCheckpointing();

		// ====================== Source ========================
		final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
			.setHost("localhost")
			.setVirtualHost("/")
			.setUserName("guest")
			.setPassword("guest")
			.setPort(5672)
    		.build();

		final DataStream<String> stream = env
			.addSource(new RMQSource<String>(
				connectionConfig,
				// config for the RabbitMQ connection
				"pub",
				// name of the RabbitMQ queue to consume
				true,
				// use correlation ids; can be false if only at-least-once is required
				new SimpleStringSchema()))   // deserialization schema to turn messages into Java objects
			.setParallelism(1);

		DataStream<String> mappedMessages = stream
			.map(new MapFunction<String, String>() {
				public String map(String message) {
					System.out.println(message);
					return message;
				}
			});

		// ====================== SINK ========================
		stream.addSink(new RMQSink<String>(
			connectionConfig,            // config for the RabbitMQ connection
			"sub",                 // name of the RabbitMQ queue to send messages to
			new SimpleStringSchema()));  // serialization schema to turn Java objects to messages


		env.execute("RabbitMQ");
	}
}
