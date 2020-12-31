package org.apache.flink.connector.rabbitmq2.source.reader;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.rabbitmq2.source.split.RabbitMQPartitionSplitState;

public class RabbitMQRecordEmitter<T> implements RecordEmitter<Tuple2<T, Long>, T, RabbitMQPartitionSplitState> {
	@Override
	public void emitRecord(
		Tuple2<T, Long> element,
		SourceOutput<T> sourceOutput,
		RabbitMQPartitionSplitState rabbitMQPartitionSplitState) throws Exception {
		sourceOutput.collect(element.f0, element.f1);
		// we do not need the splitstate
	}
}
