package org.apache.flink.connector.rabbitmq2.sink;

import org.apache.flink.api.connector.sink.Committer;

import java.io.IOException;
import java.util.List;

public class RabbitMQSinkCommiter implements Committer {
    @Override
    public void close() throws Exception {

    }

    @Override
    public List commit(List committables) throws IOException {
        return null;
    }
}
