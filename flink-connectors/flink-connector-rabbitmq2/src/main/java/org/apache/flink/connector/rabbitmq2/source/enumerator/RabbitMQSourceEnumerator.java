package org.apache.flink.connector.rabbitmq2.source.enumerator;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.rabbitmq2.ConsistencyMode;
import org.apache.flink.connector.rabbitmq2.source.split.RabbitMQPartitionSplit;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;

/** TODO. */
public class RabbitMQSourceEnumerator
        implements SplitEnumerator<RabbitMQPartitionSplit, RabbitMQSourceEnumState> {
    private final SplitEnumeratorContext<RabbitMQPartitionSplit> context;
    private final ConsistencyMode consistencyMode;
    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQSourceEnumerator.class);

    // AT_MOST_ONCE and AT_LEAST_ONCE do not store anything in the split state, thus
    // all system wide splits are always equivalent. EXACTLY_ONCE uses the split state
    // for storing message ids but this only works with source parallelism of one and therefore,
    // again, only one system wide split will exist.
    private RabbitMQPartitionSplit split;

    public RabbitMQSourceEnumerator(
            SplitEnumeratorContext<RabbitMQPartitionSplit> context,
            ConsistencyMode consistencyMode,
            RMQConnectionConfig connectionConfig,
            String rmqQueueName,
            RabbitMQSourceEnumState enumState) {
        // The enumState is not used since the enumerator has no state in this architecture.
        this(context, consistencyMode, connectionConfig, rmqQueueName);
    }

    public RabbitMQSourceEnumerator(
            SplitEnumeratorContext<RabbitMQPartitionSplit> context,
            ConsistencyMode consistencyMode,
            RMQConnectionConfig connectionConfig,
            String rmqQueueName) {
        this.context = context;
        this.consistencyMode = consistencyMode;
        this.split = new RabbitMQPartitionSplit(connectionConfig, rmqQueueName);
    }

    @Override
    public void start() {
        System.out.println("Start ENUMERATOR");
    }

    @Override
    public void handleSplitRequest(int i, @Nullable String s) {
        LOG.info("Split request from reader " + i);
        assignSplitToReader(i, split);
    }

    @Override
    public void addSplitsBack(List<RabbitMQPartitionSplit> list, int i) {
        LOG.info("Splits returned from reader " + i);
        if (list.size() == 0) {
            return;
        }
        // Every Source Reader will only receive one splits, thus we will never get back more.
        assert list.size() == 1;
        split = list.get(0);
    }

    /**
     * In the case of exactly-once multiple readers are not allowed.
     *
     * @param i reader id
     */
    @Override
    public void addReader(int i) {}

    /** @return enum state object */
    @Override
    public RabbitMQSourceEnumState snapshotState() {
        return new RabbitMQSourceEnumState();
    }

    @Override
    public void close() {}

    private void assignSplitToReader(int readerId, RabbitMQPartitionSplit split) {
        if (consistencyMode == ConsistencyMode.EXACTLY_ONCE && context.currentParallelism() > 1) {
            throw new FlinkRuntimeException(
                    "The consistency mode is exactly-once and more than one source reader was created. "
                            + "For exactly once a parallelism higher than one is forbidden.");
        }

        SplitsAssignment<RabbitMQPartitionSplit> assignment =
                new SplitsAssignment<>(split, readerId);
        context.assignSplits(assignment);
    }
}
