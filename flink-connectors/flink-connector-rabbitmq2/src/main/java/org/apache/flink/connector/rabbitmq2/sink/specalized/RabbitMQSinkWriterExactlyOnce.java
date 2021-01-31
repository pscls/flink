package org.apache.flink.connector.rabbitmq2.sink.specalized;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.rabbitmq2.sink.RabbitMQSinkState;
import org.apache.flink.connector.rabbitmq2.sink.RabbitMQSinkWriterBase;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RabbitMQSinkWriterExactlyOnce<T> extends RabbitMQSinkWriterBase<T> {

    private List<byte[]> messages;

    public RabbitMQSinkWriterExactlyOnce(
            RMQConnectionConfig connectionConfig,
            String queueName,
            SerializationSchema<T> serializationSchema,
            List<RabbitMQSinkState> states) {
        super(connectionConfig, queueName, serializationSchema);
        messages = new ArrayList<>();
        initWithState(states);
    }

    private void initWithState(List<RabbitMQSinkState> states) {
        System.out.println("Init with state");
        for (RabbitMQSinkState state : states) {
            // TODO: assign messages
        }
    }

    protected Channel setupChannel(Connection rmqConnection) throws IOException {
        Channel rmqChannel = super.setupChannel(rmqConnection);
        rmqChannel.txSelect();
        return rmqChannel;
    }

    @Override
    public void write(T element, Context context) throws IOException {
        byte[] msg = serializationSchema.serialize(element);
        messages.add(msg);
    }

    @Override
    public List<RabbitMQSinkState> snapshotState() throws IOException {
//        System.out.println("Store " + outstandingConfirms.values().stream().toArray().length + " message into checkpoint.");
        commitMessages();
        return Collections.singletonList(new RabbitMQSinkState(messages));
    }

    private boolean commitMessages() {
        try {
            List<byte[]> messagesToSend = new ArrayList<>(messages);
            for (byte[] msg : messagesToSend) {
                rmqChannel.basicPublish("", queueName, null, msg);
            }
            rmqChannel.txCommit();
            messages.subList(0, messagesToSend.size()).clear();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }
}
