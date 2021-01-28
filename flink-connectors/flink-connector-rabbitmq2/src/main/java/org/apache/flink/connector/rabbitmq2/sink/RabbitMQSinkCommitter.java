package org.apache.flink.connector.rabbitmq2.sink;

import org.apache.flink.api.connector.sink.Committer;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class RabbitMQSinkCommitter implements Committer<RabbitMQSinkCommittable> {
    @Override
    public void close() throws Exception {

    }

    @Override
    public List<RabbitMQSinkCommittable> commit(List<RabbitMQSinkCommittable> committables) throws IOException {
        return Collections.emptyList();
    }
}
