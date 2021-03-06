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
import java.util.concurrent.TimeUnit;

/** TODO. */
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
        super(
                connectionConfig,
                queueName,
                serializationSchema,
                publishOptions,
                maxRetry,
                returnListener);
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
    public void write(T element, Context context) {
        System.out.println("Write: " + element);
        messages.add(new SinkMessage<>(element, serializationSchema.serialize(element)));
    }

    @Override
    public List<RabbitMQSinkWriterState<T>> snapshotState() {
        System.out.println("Snapshot State");
        commitMessages();
        return Collections.singletonList(new RabbitMQSinkWriterState<>(messages));
    }

    private void commitMessages() {
        List<SinkMessage<T>> messagesToSend = new ArrayList<>(messages);
        messages.subList(0, messagesToSend.size()).clear();
        System.out.println("Commit: " + messagesToSend);
        try {
            for (SinkMessage<T> msg : messagesToSend) {
                super.send(msg);
            }
            rmqChannel.txCommit();
            LOG.info("Successfully committed {} messages.", messagesToSend.size());
        } catch (IOException e) {
            LOG.error("Error during commit of {} messages. Rollback Messages. Error: {}", messagesToSend.size(), e.getMessage());
            messages.addAll(0, messagesToSend);
            e.printStackTrace();
        }
    }
}
