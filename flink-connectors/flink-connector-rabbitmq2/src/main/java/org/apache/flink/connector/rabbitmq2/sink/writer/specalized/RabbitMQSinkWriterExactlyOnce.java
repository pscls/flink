package org.apache.flink.connector.rabbitmq2.sink.writer.specalized;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.rabbitmq2.sink.RabbitMQSinkPublishOptions;
import org.apache.flink.connector.rabbitmq2.sink.SinkMessage;
import org.apache.flink.connector.rabbitmq2.sink.state.RabbitMQSinkWriterState;
import org.apache.flink.connector.rabbitmq2.sink.writer.RabbitMQSinkWriterBase;
import org.apache.flink.streaming.connectors.rabbitmq.SerializableReturnListener;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RabbitMQSinkWriterExactlyOnce<T> extends RabbitMQSinkWriterBase<T> {

    private List<SinkMessage<T>> messages;

    public RabbitMQSinkWriterExactlyOnce(
            RMQConnectionConfig connectionConfig,
            String queueName,
            SerializationSchema<T> serializationSchema,
            RabbitMQSinkPublishOptions<T> publishOptions,
            int maxRetry,
            SerializableReturnListener returnListener,
            List<RabbitMQSinkWriterState<T>> states) {
        super(connectionConfig, queueName, serializationSchema, publishOptions, maxRetry, returnListener);
        messages = new ArrayList<>();
        initWithState(states);
    }

    private void initWithState(List<RabbitMQSinkWriterState<T>> states) {
        System.out.println("Init with state");
        List<SinkMessage<T>> messages = new ArrayList<>();
        for (RabbitMQSinkWriterState<T> state : states) {
            messages.addAll(state.getOutstandingMessages());
        }
        this.messages = messages;
    }

    protected Channel setupChannel(Connection rmqConnection) throws IOException {
        Channel rmqChannel = super.setupChannel(rmqConnection);
        rmqChannel.txSelect();
        return rmqChannel;
    }

    @Override
    public void write(T element, Context context) throws IOException {
        messages.add(new SinkMessage<>(element));
    }

    @Override
    public List<RabbitMQSinkWriterState<T>> snapshotState() throws IOException {
//        System.out.println("Store " + outstandingConfirms.values().stream().toArray().length + " message into checkpoint.");
        commitMessages();
        return Collections.singletonList(new RabbitMQSinkWriterState<T>(messages));
    }

    private void commitMessages() {
        try {
            List<SinkMessage<T>> messagesToSend = new ArrayList<>(messages);
            for (SinkMessage<T> msg : messagesToSend) {
                super.send(msg);
            }
            rmqChannel.txCommit();
            messages.subList(0, messagesToSend.size()).clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
