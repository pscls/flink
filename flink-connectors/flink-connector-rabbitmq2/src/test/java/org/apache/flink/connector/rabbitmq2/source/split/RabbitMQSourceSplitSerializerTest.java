package org.apache.flink.connector.rabbitmq2.source.split;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** Test the source split serializer. */
public class RabbitMQSourceSplitSerializerTest {

    private RabbitMQSourceSplit getSourceSplit() {

        String queueName = "exampleQueueName";
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            ids.add(Integer.toString(i));
        }
        Set<String> correlationIds = new HashSet<>(ids);
        return new RabbitMQSourceSplit(null, queueName, correlationIds);
    }

    @Test
    public void testSplitSerializer() {
        RabbitMQSourceSplit split = getSourceSplit();
        RabbitMQSourceSplitSerializer serializer = new RabbitMQSourceSplitSerializer();
        RabbitMQSourceSplit deserializedSplit = null;
        try {
            byte[] serializedSplit = serializer.serialize(split);
            deserializedSplit = serializer.deserialize(0, serializedSplit);
        } catch (IOException e) {
            e.printStackTrace();
        }

        assertNotNull(deserializedSplit);
        assertEquals(split.splitId(), deserializedSplit.splitId());
        assertEquals(split.getCorrelationIds(), deserializedSplit.getCorrelationIds());
        assertEquals(split.getQueueName(), deserializedSplit.getQueueName());
        assertEquals(split.getConnectionConfig(), deserializedSplit.getConnectionConfig());
    }
}
