package org.apache.flink.connector.rabbitmq2.source.common;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** TODO. */
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
