package org.apache.flink.connector.rabbitmq2.source.common;

import org.apache.flink.api.connector.source.SplitEnumerator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

public class EmptyEnumerator implements SplitEnumerator<EmptyPartitionSplit, EmptyEnumState> {
	@Override
	public void start() {

	}

	@Override
	public void handleSplitRequest(int i, @Nullable String s) {

	}

	@Override
	public void addReader(int i) {
	}

	@Override
	public EmptyEnumState snapshotState() {
		return new EmptyEnumState();
	}

	@Override
	public void close() {

	}

	@Override
	public void addSplitsBack(List list, int i) {

	}
}
