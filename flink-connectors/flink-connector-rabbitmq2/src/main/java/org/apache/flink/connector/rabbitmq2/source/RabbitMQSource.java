package org.apache.flink.connector.rabbitmq2.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.rabbitmq2.ConsistencyMode;
import org.apache.flink.connector.rabbitmq2.source.enumerator.RabbitMQSourceEnumState;
import org.apache.flink.connector.rabbitmq2.source.enumerator.RabbitMQSourceEnumStateSerializer;
import org.apache.flink.connector.rabbitmq2.source.enumerator.RabbitMQSourceEnumerator;
import org.apache.flink.connector.rabbitmq2.source.reader.specialized.RabbitMQSourceReaderAtLeastOnce;
import org.apache.flink.connector.rabbitmq2.source.reader.specialized.RabbitMQSourceReaderAtMostOnce;
import org.apache.flink.connector.rabbitmq2.source.reader.specialized.RabbitMQSourceReaderExactlyOnce;
import org.apache.flink.connector.rabbitmq2.source.split.RabbitMQPartitionSplit;
import org.apache.flink.connector.rabbitmq2.source.split.RabbitMQPartitionSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import com.esotericsoftware.minlog.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** TODO. */
public class RabbitMQSource<T>
        implements Source<T, RabbitMQPartitionSplit, RabbitMQSourceEnumState>,
                ResultTypeQueryable<T> {
    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQSource.class);

    private final RMQConnectionConfig connectionConfig;
    private final String queueName;
    private final ConsistencyMode consistencyMode;
    protected DeserializationSchema<T> deliveryDeserializer;

    public RabbitMQSource(
            RMQConnectionConfig connectionConfig,
            String queueName,
            DeserializationSchema<T> deserializationSchema,
            ConsistencyMode consistencyMode) {
        this.connectionConfig = connectionConfig;
        this.queueName = queueName;
        this.deliveryDeserializer = deserializationSchema;
        this.consistencyMode = consistencyMode;

        System.out.println("Create SOURCE");
    }

    /**
     * @param <T> type of the source
     * @return a source builder
     * @see RabbitMQSourceBuilder
     */
    public static <T> RabbitMQSourceBuilder<T> builder() {
        return new RabbitMQSourceBuilder<>();
    }

    /**
     * The boundedness is always continuous unbounded.
     *
     * @return
     * @see Boundedness
     */
    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    /**
     * Returns a new initialized source-reader of the source's consistency mode.
     *
     * @param sourceReaderContext context which the reader will be created in
     * @return a source reader of the right type
     * @see RabbitMQSourceReaderBase
     */
    @Override
    public SourceReader<T, RabbitMQPartitionSplit> createReader(
            SourceReaderContext sourceReaderContext) {
        System.out.println("Create READER");
        LOG.info("New Source Reader of type " + consistencyMode + " requested.");
        switch (consistencyMode) {
            case AT_MOST_ONCE:
                return new RabbitMQSourceReaderAtMostOnce<>(
                        sourceReaderContext, deliveryDeserializer);
            case AT_LEAST_ONCE:
                return new RabbitMQSourceReaderAtLeastOnce<>(
                        sourceReaderContext, deliveryDeserializer);
            case EXACTLY_ONCE:
                return new RabbitMQSourceReaderExactlyOnce<>(
                        sourceReaderContext, deliveryDeserializer);
            default:
                Log.error("The requested reader of type " + consistencyMode + " is not supported");
                return null;
        }
    }

    /**
     * @param splitEnumeratorContext context which the enumerator will be created in
     * @return a new split enumerator
     * @see SplitEnumerator
     */
    @Override
    public SplitEnumerator<RabbitMQPartitionSplit, RabbitMQSourceEnumState> createEnumerator(
            SplitEnumeratorContext<RabbitMQPartitionSplit> splitEnumeratorContext) {
        System.out.println("Create ENUMERATOR");
        return new RabbitMQSourceEnumerator(
                splitEnumeratorContext, consistencyMode, connectionConfig, queueName);
    }

    /**
     * @param splitEnumeratorContext context which the enumerator will be created in
     * @param enumState enum state the
     * @return a new split enumerator
     * @see SplitEnumerator
     */
    @Override
    public SplitEnumerator<RabbitMQPartitionSplit, RabbitMQSourceEnumState> restoreEnumerator(
            SplitEnumeratorContext<RabbitMQPartitionSplit> splitEnumeratorContext,
            RabbitMQSourceEnumState enumState) {
        return new RabbitMQSourceEnumerator(
                splitEnumeratorContext, consistencyMode, connectionConfig, queueName, enumState);
    }

    /**
     * @return a simple serializer for a RabbitMQPartitionSplit
     * @see SimpleVersionedSerializer
     */
    @Override
    public SimpleVersionedSerializer<RabbitMQPartitionSplit> getSplitSerializer() {
        return new RabbitMQPartitionSplitSerializer();
    }

    /**
     * @return a simple serializer for a RabbitMQSourceEnumState
     * @see SimpleVersionedSerializer
     */
    @Override
    public SimpleVersionedSerializer<RabbitMQSourceEnumState> getEnumeratorCheckpointSerializer() {
        return new RabbitMQSourceEnumStateSerializer();
    }

    /**
     * @return type information
     * @see TypeInformation
     */
    @Override
    public TypeInformation<T> getProducedType() {
        return deliveryDeserializer.getProducedType();
    }
}
