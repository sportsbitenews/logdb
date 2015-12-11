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
package org.araqne.logdb.query.engine;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicLong;

import org.araqne.codec.EncodingRule;
import org.araqne.logdb.QueryResult;
import org.araqne.logdb.QueryResultCallback;
import org.araqne.logdb.QueryResultClosedException;
import org.araqne.logdb.QueryResultConfig;
import org.araqne.logdb.QueryResultSet;
import org.araqne.logdb.QueryResultStorage;
import org.araqne.logdb.QueryStatusCallback;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logstorage.Log;
import org.araqne.logstorage.LogFlushCallback;
import org.araqne.logstorage.LogFlushCallbackArgs;
import org.araqne.logstorage.file.LogFileReader;
import org.araqne.logstorage.file.LogFileWriter;
import org.araqne.logstorage.file.LogRecord;
import org.araqne.logstorage.file.LogRecordCursor;
import org.araqne.storage.localfile.LocalFilePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryResultImpl implements QueryResult, LogFlushCallback {
	private final Logger logger = LoggerFactory.getLogger(QueryResultImpl.class);
	private LogFileWriter writer;
	private AtomicLong counter = new AtomicLong();
	private AtomicLong flushed = new AtomicLong();

	/**
	 * do NOT directly lock on log file writer. input should be serialized at caller side. inner block-able
	 * caller run policy can cause deadlock.
	 */
	private Object writerLock = new Object();

	/**
	 * index and data file is deleted by user request
	 */
	private volatile boolean purged;

	/**
	 * prevent indirect recursive query.stop()
	 */
	private volatile boolean stopRequested;

	private volatile boolean writerClosed;
	private volatile Date eofDate;
	private volatile boolean streaming;

	private QueryResultConfig config;
	private QueryResultStorage resultStorage;
	private Set<QueryResultCallback> resultCallbacks = new CopyOnWriteArraySet<QueryResultCallback>();

	public QueryResultImpl(QueryResultConfig config, QueryResultStorage resultStorage) throws IOException {
		this.config = config;
		this.resultStorage = resultStorage;
	}

	@Override
	public Date getEofDate() {
		return eofDate;
	}

	@Override
	public long getCount() {
		return counter.get();
	}

	@Override
	public boolean isThreadSafe() {
		return false;
	}

	@Override
	public void onRow(Row row) {
		long count = counter.incrementAndGet();
		try {
			if (!streaming) {
				synchronized (writerLock) {
					if (writerClosed)
						throw new QueryResultClosedException();

					writer.write(new Log("$Result$", new Date(), count, row.map()));
				}
			}
		} catch (IOException e) {
			// cancel query when disk is full
			synchronized (writerLock) {
				if (!stopRequested && writer.isLowDisk()) {
					stopRequested = true;
					config.getQuery().cancel(QueryStopReason.LowDisk);
				}
			}
			throw new IllegalStateException(e);
		}

		for (QueryResultCallback c : resultCallbacks) {
			try {
				c.onRow(config.getQuery(), row);
			} catch (Throwable t) {
				logger.warn("araqne logdb: result callback should not throw any exception", t);
			}
		}
	}

	private void invokeCloseCallbacks(QueryStopReason reason) {
		for (QueryResultCallback c : resultCallbacks) {
			try {
				c.onClose(config.getQuery(), reason);
			} catch (Throwable t) {
				logger.error("araqne logdb: cannot handle QueryResult.onStop()", t);
			}
		}
	}

	@Override
	public void onRowBatch(RowBatch rowBatch) {

		try {
			synchronized (writerLock) {
				if (writerClosed)
					throw new IllegalStateException("result writer is already closed");

				if (rowBatch.selectedInUse) {
					for (int i = 0; i < rowBatch.size; i++) {
						Row row = rowBatch.rows[rowBatch.selected[i]];
						long count = counter.incrementAndGet();
						if (!streaming)
							writer.write(new Log("$Result$", new Date(), count, row.map()));
					}
				} else {
					for (int i = 0; i < rowBatch.size; i++) {
						Row row = rowBatch.rows[i];

						long count = counter.incrementAndGet();
						if (!streaming)
							writer.write(new Log("$Result$", new Date(), count, row.map()));
					}
				}
			}

			for (QueryResultCallback c : resultCallbacks) {
				try {
					c.onRowBatch(config.getQuery(), rowBatch);
				} catch (Throwable t) {
					logger.warn("araqne logdb: result callback should not throw any exception", t);
				}
			}
		} catch (IOException e) {
			// cancel query when disk is full
			synchronized (writerLock) {
				if (!stopRequested && writer.isLowDisk()) {
					stopRequested = true;
					config.getQuery().cancel(QueryStopReason.LowDisk);
				}
			}
			throw new IllegalStateException(e);
		}
	}

	@Override
	public QueryResultSet getResultSet() throws IOException {
		if (purged) {
			String msg = "query [" + config.getQuery().getId() + "] result file is already purged";
			throw new IOException(msg);
		}

		syncWriter();

		LogFileReader reader = null;
		try {
			reader = resultStorage.createReader(config);
			return new LogResultSetImpl(resultStorage.getName(), reader, flushed.get());
		} catch (Throwable t) {
			if (reader != null)
				reader.close();
			throw new IOException(t);
		}
	}

	@Override
	public void syncWriter() throws IOException {
		synchronized (writerLock) {
			if(writer != null)
				writer.flush();
				writer.sync();
		}
	}

	@Override
	public void closeWriter() {
		if (writerClosed)
			return;

		writerClosed = true;

		try {
			synchronized (writerLock) {
				if (writer != null)
					writer.close();
			}
		} catch (IOException e) {
		}

		eofDate = new Date();

		for (QueryStatusCallback callback : config.getQuery().getCallbacks().getStatusCallbacks()) {
			try {
				callback.onChange(config.getQuery());
			} catch (Throwable t) {
				int queryId = config.getQuery().getId();
				logger.warn("araqne logdb: query [" + queryId + "] status callback should not throw any exception", t);
			}
		}

		invokeCloseCallbacks(QueryStopReason.End);
	}

	@Override
	public void purge() {
		if (purged)
			return;

		purged = true;
		resultCallbacks.clear();

		// delete files
		synchronized (writerLock) {
			try {
				if (writer != null)
					writer.close();
			} catch (IOException e) {
				int queryId = config.getQuery().getId();
				logger.warn("araqne logdb: cannot close query [" + queryId + "] result writer", e);
			}

			if (writer != null)
				writer.purge();
		}
	}

	@Override
	public boolean isStreaming() {
		return streaming;
	}

	@Override
	public void setStreaming(boolean streaming) {
		this.streaming = streaming;
	}

	@Override
	public Set<QueryResultCallback> getResultCallbacks() {
		return resultCallbacks;
	}

	private static class LogResultSetImpl implements QueryResultSet {
		private String storageName;
		private LogFileReader reader;
		private LogRecordCursor cursor;
		private long count;

		// assume result is stored in local storage
		public LogResultSetImpl(String storageName, LogFileReader reader, long count) throws IOException {
			this.storageName = storageName;
			this.reader = reader;
			this.cursor = reader.getCursor(true);
			this.count = count;
		}

		@Override
		public String getStorageName() {
			return storageName;
		}

		@Override
		public File getIndexPath() {
			return ((LocalFilePath) reader.getIndexPath()).getFile();
		}

		@Override
		public File getDataPath() {
			return ((LocalFilePath) reader.getDataPath()).getFile();
		}

		@Override
		public long size() {
			return count;
		}

		@Override
		public boolean hasNext() {
			return cursor.hasNext();
		}

		@Override
		public Map<String, Object> next() {
			LogRecord next = cursor.next();
			return EncodingRule.decodeMap(next.getData());
		}

		@Override
		public void reset() {
			cursor.reset();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void skip(long n) {
			cursor.skip(n);
		}

		@Override
		public void close() {
			reader.close();
		}
	}

	@Override
	public void onFlushCompleted(LogFlushCallbackArgs arg) {
		flushed.addAndGet(arg.getLogs().size());
	}

	@Override
	public void onFlush(LogFlushCallbackArgs arg) {
	}

	@Override
	public void onFlushException(LogFlushCallbackArgs arg, Throwable t) {
	}

	@Override
	public void openWriter() throws IOException {
		writer = resultStorage.createWriter(config);
		writer.getCallbackSet().get(LogFlushCallback.class).add(this);
	}
}
