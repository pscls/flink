package org.apache.flink.connector.rabbitmq2.source.split;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.rabbitmq2.source.RabbitMQSource;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import java.util.HashSet;
import java.util.Set;

/**
 * This split is passed by the RabbitMQSourceEnumerator to the SourceReader. It contains the
 * configuration for the connection and the name of the queue to connect to. In case of exactly-once
 * the correlation ids for deduplication of messages might contain data. They are fIt might contain
 * data ife single reader fails and a new reader needs to be * created.
 */
public class RabbitMQPartitionSplit implements SourceSplit {

    private final RMQConnectionConfig connectionConfig;
    private final String rmqQueueName;
    private Set<String> correlationIds;

    public RabbitMQPartitionSplit(RMQConnectionConfig connectionConfig, String rmqQueueName) {
        this(connectionConfig, rmqQueueName, new HashSet<>());
    }

    public RabbitMQPartitionSplit(
            RMQConnectionConfig connectionConfig, String rmqQueueName, Set<String> correlationIds) {
        this.connectionConfig = connectionConfig;
        this.rmqQueueName = rmqQueueName;
        this.correlationIds = correlationIds;
    }

    public RabbitMQPartitionSplit clone() {
        return new RabbitMQPartitionSplit(connectionConfig, rmqQueueName, new HashSet<>(correlationIds));
    }

    public Set<String> getCorrelationIds() {
        return correlationIds;
    }

    public String getQueueName() {
        return rmqQueueName;
    }

    public void setCorrelationIds(Set<String> newCorrelationIds) {
        correlationIds = newCorrelationIds;
    }

    public RMQConnectionConfig getConnectionConfig() {
        return connectionConfig;
    }

    @Override
    public String splitId() {
        return correlationIds.toString(); // TODO: find something better
    }
}
