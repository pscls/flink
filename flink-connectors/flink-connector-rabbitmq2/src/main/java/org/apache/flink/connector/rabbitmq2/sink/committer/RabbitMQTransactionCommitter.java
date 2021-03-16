package org.apache.flink.connector.rabbitmq2.sink.committer;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.connector.rabbitmq2.common.RabbitMQConnectionConfig;
import org.apache.flink.connector.rabbitmq2.sink.common.RabbitMQSinkConnection;
import org.apache.flink.connector.rabbitmq2.sink.common.RabbitMQSinkMessageWrapper;
import org.apache.flink.connector.rabbitmq2.sink.common.RabbitMQSinkPublishOptions;
import org.apache.flink.connector.rabbitmq2.sink.common.SerializableReturnListener;
import org.apache.flink.connector.rabbitmq2.sink.state.RabbitMQSinkWriterState;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// TODO: Document the new architecture
public class RabbitMQTransactionCommitter<T> extends RabbitMQSinkConnection<T>
        implements Committer<RabbitMQSinkWriterState<T>> {

    public RabbitMQTransactionCommitter(
            RabbitMQConnectionConfig connectionConfig,
            String queueName,
            RabbitMQSinkPublishOptions<T> publishOptions,
            SerializableReturnListener returnListener)
            throws Exception {
        super(connectionConfig, queueName, publishOptions, returnListener);
        configureChannel();
    }

    private void configureChannel()
            throws IOException {
        // Put the channel in commit mode
        getRmqChannel().txSelect();
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
        getRmqChannel().txCommit();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
