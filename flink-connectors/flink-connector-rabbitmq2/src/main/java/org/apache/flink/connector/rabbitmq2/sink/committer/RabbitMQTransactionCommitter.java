package org.apache.flink.connector.rabbitmq2.sink.committer;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.connector.rabbitmq2.common.RabbitMQConnectionConfig;
import org.apache.flink.connector.rabbitmq2.sink.common.RabbitMQSinkMessageWrapper;
import org.apache.flink.connector.rabbitmq2.sink.common.RabbitMQSinkPublishOptions;
import org.apache.flink.connector.rabbitmq2.sink.common.SerializableReturnListener;
import org.apache.flink.connector.rabbitmq2.sink.state.RabbitMQSinkWriterState;
import org.apache.flink.connector.rabbitmq2.sink.writer.RabbitMQSinkWriterBase;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RabbitMQTransactionCommitter<T> extends RabbitMQSinkWriterBase<T>
        implements Committer<RabbitMQSinkWriterState<T>> {

    public RabbitMQTransactionCommitter(
            RabbitMQConnectionConfig connectionConfig,
            String queueName,
            SerializationSchema<T> serializationSchema,
            RabbitMQSinkPublishOptions<T> publishOptions,
            SerializableReturnListener returnListener) {
        super(connectionConfig, queueName, serializationSchema, publishOptions, returnListener);
    }

    @Override
    protected Channel setupChannel(Connection rmqConnection) throws IOException {
        Channel rmqChannel = super.setupChannel(rmqConnection);
        // Put the channel in commit mode
        rmqChannel.txSelect();
        return rmqChannel;
    }

    @Override
    public List<RabbitMQSinkWriterState<T>> commit(List<RabbitMQSinkWriterState<T>> committables) {
        List<RabbitMQSinkWriterState<T>> failedCommittables = new ArrayList<>();
        for (RabbitMQSinkWriterState<T> committable : committables) {
            List<RabbitMQSinkMessageWrapper<T>> messagesToSend =
                    committable.getOutstandingMessages();
            try {
                commitMessages(messagesToSend);
                LOG.info("Successfully committed {} messages.", messagesToSend.size());
            } catch (IOException e) {
                LOG.error(
                        "Error during commit of {} messages. Error: {}",
                        messagesToSend.size(),
                        e.getMessage());
                failedCommittables.add(committable);
            }
        }
        return failedCommittables;
    }

    private void commitMessages(List<RabbitMQSinkMessageWrapper<T>> messagesToSend)
            throws IOException {
        for (RabbitMQSinkMessageWrapper<T> msg : messagesToSend) {
            super.send(msg);
        }
        rmqChannel.txCommit();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
