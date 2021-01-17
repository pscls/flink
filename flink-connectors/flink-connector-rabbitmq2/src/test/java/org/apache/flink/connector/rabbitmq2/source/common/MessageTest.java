package org.apache.flink.connector.rabbitmq2.source.common;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class MessageTest {

	@Test
	public void testGetAttributes() {
		String correlationId = "correlationId";
		long deliveryTag = 1;
		String body = "";
		Message<String> m = new Message<>(1, correlationId, body);
		assertEquals(correlationId, m.getCorrelationId());
		assertEquals(deliveryTag, m.getDeliveryTag());
		assertEquals(body, m.getMessage());
	}
}
