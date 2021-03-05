package org.apache.flink.connector.rabbitmq2.source.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.Set;

/**
 * The {@link org.apache.flink.core.io.SimpleVersionedSerializer serializer} for {@link
 * RabbitMQSourceSplit}.
 *
 * @see RabbitMQSourceSplit
 */
public class RabbitMQSourceSplitSerializer
        implements SimpleVersionedSerializer<RabbitMQSourceSplit> {
    private static final int CURRENT_VERSION = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(RabbitMQSourceSplit rabbitMQSourceSplit) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos);
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(out)) {
            objectOutputStream.writeObject(rabbitMQSourceSplit.getConnectionConfig());
            out.writeUTF(rabbitMQSourceSplit.getQueueName());
            writeStringSet(out, rabbitMQSourceSplit.getCorrelationIds());
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public RabbitMQSourceSplit deserialize(int i, byte[] bytes) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                DataInputStream in = new DataInputStream(bais);
                ObjectInputStream objectInputStream = new ObjectInputStream(in)) {
            RMQConnectionConfig config = (RMQConnectionConfig) objectInputStream.readObject();
            String queueName = in.readUTF();
            Set<String> correlationIds = readStringSet(in);
            return new RabbitMQSourceSplit(config, queueName, correlationIds);
        } catch (ClassNotFoundException e) {
            throw new IOException(e.getException());
        }
    }

    private static void writeStringSet(DataOutputStream out, Set<String> strings)
            throws IOException {
        out.writeInt(strings.size());
        for (String string : strings) {
            out.writeUTF(string);
        }
    }

    private static Set<String> readStringSet(DataInputStream in) throws IOException {
        final int len = in.readInt();
        final Set<String> strings = new HashSet<>();
        for (int i = 0; i < len; i++) {
            strings.add(in.readUTF());
        }
        return strings;
    }
}
