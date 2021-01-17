package org.apache.flink.connector.rabbitmq2.source.reader;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.rabbitmq2.source.common.Message;

import com.rabbitmq.client.Delivery;

import org.apache.flink.connector.rabbitmq2.source.helper.DummyDelivery;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;


public class RabbitMQCollectorTest {

	@Test
	public void testDefaultMaxCapacity() {
		RabbitMQCollector<String> collector = new RabbitMQCollector<>(new SimpleStringSchema());
		assertEquals(Integer.MAX_VALUE, collector.getMessageQueue().remainingCapacity());
	}

	@Test
	public void testProcessMessage () {
		RabbitMQCollector<String> collector = new RabbitMQCollector<>(new SimpleStringSchema());

		int initCapacity = collector.getMessageQueue().remainingCapacity();
		int numOfMessages = 10;
		for (int i = 0; i < numOfMessages; i++) {
			try {
				collector.processMessage(DummyDelivery.build());
			} catch (IOException e) {
				fail("Unable to process message");
			}
		}

		assertEquals(numOfMessages, collector.getNumberOfUnpolledMessages());
		assertEquals(initCapacity - numOfMessages, collector.getMessageQueue().remainingCapacity());
	}

	@Test
	public void testPollMessage() throws IOException {
		RabbitMQCollector<String> collector = new RabbitMQCollector<>(new SimpleStringSchema());
		collector.processMessage(DummyDelivery.build());

		assertTrue(collector.hasUnpolledMessages());

		Message<String> message = collector.pollMessage();
		assertEquals(DummyDelivery.defaultCorrelationId, message.getCorrelationId());
		assertEquals(DummyDelivery.defaultDeliveryTag, message.getDeliveryTag());
		assertEquals(DummyDelivery.defaultMessage, message.getMessage());
		assertEquals(0, collector.getNumberOfUnpolledMessages());
		assertFalse(collector.hasUnpolledMessages());
	}
}
