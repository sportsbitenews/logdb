/*
 * Copyright 2011 Future Systems
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.araqne.logdb;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class QueryCommand {
	public static enum Status {
		Waiting, Running, End, Finalizing
	}

	protected Query query;
	private boolean cancelled = false;

	protected RowPipe output;
	private AtomicLong outputCount = new AtomicLong();
	protected volatile Status status = Status.Waiting;
	private AtomicBoolean startCalled = new AtomicBoolean();
	private AtomicBoolean closeCalled = new AtomicBoolean();
	private ReadWriteLock rwLock = new ReentrantReadWriteLock(true);

	public QueryTask getMainTask() {
		return null;
	}

	public QueryTask getDependency() {
		return getMainTask();
	}

	public long getOutputCount() {
		return outputCount.get();
	}

	public Status getStatus() {
		return status;
	}

	public void setStatus(Status status) {
		this.status = status;
	}

	public RowPipe getOutput() {
		return output;
	}

	public void setOutput(RowPipe output) {
		this.output = output;
	}

	@Deprecated
	public boolean isCancelled() {
		return cancelled;
	}

	public abstract String getName();

	public QueryContext getContext() {
		return query.getContext();
	}

	public Query getQuery() {
		return query;
	}

	public void setQuery(Query query) {
		this.query = query;
	}

	public void onStart() {
		// override this for initialization
	}

	protected void onClose(QueryStopReason reason) {
		// override this for resource clean up
	}

	public void tryStart() {
		closeCalled.set(false);
		if (startCalled.compareAndSet(false, true))
			onStart();
	}

	public void tryClose(QueryStopReason reason) {
		if (closeCalled.compareAndSet(false, true)) {
			Lock lock = rwLock.writeLock();
			try {
				lock.lock();

				onClose(reason);
			} finally {
				lock.unlock();
			}
		}
	}

	public void onPush(Row row) {
		pushPipe(row);
	}

	// default adapter for old per-row processing
	public void onPush(RowBatch rowBatch) {
		if (rowBatch.selectedInUse) {
			for (int i = 0; i < rowBatch.size; i++) {
				Row row = rowBatch.rows[rowBatch.selected[i]];
				onPush(row);
			}
		} else {
			for (int i = 0; i < rowBatch.size; i++) {
				Row row = rowBatch.rows[i];
				onPush(row);
			}
		}
	}

	// default adapter for old per-rowbatch processing
	public void onPush(VectorizedRowBatch vbatch) {
		onPush(vbatch.toRowBatch());
	}

	protected final void pushPipe(Row row) {
		Lock lock = rwLock.readLock();
		try {
			lock.lock();

			outputCount.incrementAndGet();

			if (output != null) {
				if (output.isThreadSafe()) {
					output.onRow(row);
				} else {
					synchronized (output) {
						output.onRow(row);
					}
				}
			}
		} finally {
			lock.unlock();
		}

	}

	protected final void pushPipe(RowBatch rowBatch) {
		Lock lock = rwLock.readLock();
		try {
			lock.lock();

			outputCount.addAndGet(rowBatch.size);
			if (output != null) {
				if (output.isThreadSafe()) {
					output.onRowBatch(rowBatch);
				} else {
					synchronized (output) {
						output.onRowBatch(rowBatch);
					}
				}
			}
		} finally {
			lock.unlock();
		}
	}

	protected final void pushPipe(VectorizedRowBatch vbatch) {
		Lock lock = rwLock.readLock();
		try {
			lock.lock();

			outputCount.addAndGet(vbatch.size);
			if (output != null) {
				if (output.isThreadSafe()) {
					output.onVectorizedRowBatch(vbatch);
				} else {
					synchronized (output) {
						output.onVectorizedRowBatch(vbatch);
					}
				}
			}
		} finally {
			lock.unlock();
		}
	}

	public boolean isDriver() {
		return false;
	}

	public boolean isReducer() {
		return false;
	}

	public List<QueryCommand> getNestedCommands() {
		return new ArrayList<QueryCommand>();
	}
}
