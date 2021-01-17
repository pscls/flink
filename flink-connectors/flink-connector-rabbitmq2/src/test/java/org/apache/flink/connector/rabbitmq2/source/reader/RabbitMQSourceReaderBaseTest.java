package org.apache.flink.connector.rabbitmq2.source.reader;

import com.github.fridujo.rabbitmq.mock.MockConnectionFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import com.rabbitmq.client.ConnectionFactory;

import com.rabbitmq.client.Delivery;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.rabbitmq2.source.helper.DummyDelivery;
import org.apache.flink.connector.rabbitmq2.source.split.RabbitMQPartitionSplit;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

import org.mockito.Mockito;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.Future;


public class RabbitMQSourceReaderBaseTest {

 	private static class MockSourceReaderBase<T> extends RabbitMQSourceReaderBase<T> {
 		public MockSourceReaderBase(
			SourceReaderContext sourceReaderContext,
			RMQConnectionConfig rmqConnectionConfig,
			DeserializationSchema<T> deliveryDeserializer) {
			super(sourceReaderContext, rmqConnectionConfig, deliveryDeserializer);
		}

		@Override
		protected boolean isAutoAck() { return false; }

		@Override
		protected Connection setupConnection() throws IOException, TimeoutException {
			ConnectionFactory factory = new MockConnectionFactory();
			return factory.newConnection();
		}

		@Override
		protected Channel setupChannel(Connection rmqConnection) {
 			Channel channel = Mockito.spy(Channel.class);
 			return channel;
		}

		public void setupRabbitMQConnection () {
			setupRabbitMQ();
		}

		public void addDelivery() throws IOException {
			addDelivery(DummyDelivery.build());
		}

		public void addDelivery(Delivery delivery) throws IOException {
			handleMessageReceivedCallback("not used", delivery);
		}
	}

	private MockSourceReaderBase<String> reader;

	@Before
	public void setup() {
		final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
			.setHost("localhost")
			.setVirtualHost("/")
			.setUserName("guest")
			.setPassword("guest")
			.setPort(5672)
			.setPrefetchCount(1000)
			.build();
		SourceReaderContext context = Mockito.mock(SourceReaderContext.class);
		this.reader = new MockSourceReaderBase<>(
			context,
			connectionConfig,
			new SimpleStringSchema());
		this.reader.setupRabbitMQConnection();
	}

	@Test
	public void testAddSplitOverwrite() {
		RabbitMQPartitionSplit split = new RabbitMQPartitionSplit("queue");

		List<RabbitMQPartitionSplit> splits = Collections.singletonList(split);
		reader.addSplits(splits);
		assertEquals(split, reader.getSplit());

		RabbitMQPartitionSplit newSplit = new RabbitMQPartitionSplit("queue2",
			new HashSet<>(Arrays.asList("1", "2", "3")));
		reader.addSplits(Collections.singletonList(newSplit));
		assertEquals(newSplit, reader.getSplit());
	}

	@Test(expected = AssertionError.class)
	public void testAddMultipleSplitsException() {
		RabbitMQPartitionSplit split = new RabbitMQPartitionSplit("queue");

		List<RabbitMQPartitionSplit> splits = Arrays.asList(split, split);
		reader.addSplits(splits);
	}

	@Test
	public void testPollNextIsEmpty() throws IOException {
		ReaderOutput<String> output = Mockito.spy(ReaderOutput.class);
		reader.addDelivery();
		InputStatus status = reader.pollNext(output);
		assertEquals(status, InputStatus.NOTHING_AVAILABLE);
		verify(output).collect(DummyDelivery.defaultMessage);
	}

	@Test
	public void testPollIsEmpty() {
		ReaderOutput<String> output = Mockito.spy(ReaderOutput.class);
		InputStatus status = reader.pollNext(output);
		assertEquals(status, InputStatus.NOTHING_AVAILABLE);
		verify(output, never()).collect(Mockito.any());
	}

	@Test
	public void testPollNextIsAvailable() throws IOException {
		ReaderOutput<String> output = Mockito.spy(ReaderOutput.class);
		reader.addDelivery();
		reader.addDelivery();
		InputStatus status = reader.pollNext(output);
		assertEquals(status, InputStatus.MORE_AVAILABLE);
		verify(output).collect(DummyDelivery.defaultMessage);
	}

//	@Test
//	public void testPollNextConcurrently() throws IOException {
//		ReaderOutput<String> output = Mockito.spy(ReaderOutput.class);
//		reader.addDelivery();
//		reader.addDelivery();
//		int threads = 10;
//		ExecutorService service = Executors.newFixedThreadPool(threads);
//		Collection<Future<Integer>> futures = new ArrayList<>(threads);
//		for (int t = 0; t < threads; ++t) {
//			futures.add(service.submit(() -> reader.pollNext(output).hashCode())); // TODO: get message
//		}
//		Set<Integer> ids = new HashSet<>();
//		for (Future<Integer> f : futures) {
//			try {
//				ids.add(f.get());
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//			assertEquals(ids.size(), threads);
//		}
//	}

	@Test
	public void testAcknowledgeMessageIds() throws IOException {
		List<Long> sessionIds = Arrays.asList(1L, 2L, 3L);
		reader.acknowledgeMessageIds(sessionIds);

		verify(reader.getRmqChannel()).basicAck(1L, false);
		verify(reader.getRmqChannel()).basicAck(2L, false);
		verify(reader.getRmqChannel()).basicAck(3L, false);
	}

	@Test
	public void testAcknowledgeMessageId() throws IOException {
		reader.acknowledgeMessageId(1L);
		verify(reader.getRmqChannel()).basicAck(1L, false);
	}
}
