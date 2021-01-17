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
	public void getAttributes() {
		String correlationId = "correlationId";
		long deliveryTag = 1;
		Message m = new Message(1, correlationId);
		assertEquals(m.getCorrelationId(), correlationId);
	}
}
