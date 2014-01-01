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

import org.araqne.codec.EncodingRule;
import org.araqne.logdb.QueryResult;
import org.araqne.logdb.QueryResultCallback;
import org.araqne.logdb.QueryResultConfig;
import org.araqne.logdb.QueryResultSet;
import org.araqne.logdb.QueryResultStorage;
import org.araqne.logdb.QueryStatusCallback;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logstorage.Log;
import org.araqne.logstorage.file.LogFileReader;
import org.araqne.logstorage.file.LogFileWriter;
import org.araqne.logstorage.file.LogRecord;
import org.araqne.logstorage.file.LogRecordCursor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryResultImpl implements QueryResult {
	private final Logger logger = LoggerFactory.getLogger(QueryResultImpl.class);
	private LogFileWriter writer;
	private long count;

	/**
	 * do NOT directly lock on log file writer. input should be serialized at
	 * caller side. inner block-able caller run policy can cause deadlock.
	 */
	private Object writerLock = new Object();

	/**
	 * index and data file is deleted by user request
	 */
	private volatile boolean purged;

	private volatile boolean writerClosed;
	private volatile Date eofDate;

	private QueryResultConfig config;
	private QueryResultStorage resultStorage;
	private Set<QueryResultCallback> resultCallbacks = new CopyOnWriteArraySet<QueryResultCallback>();

	public QueryResultImpl(QueryResultConfig config, QueryResultStorage resultStorage) throws IOException {
		this.config = config;
		this.resultStorage = resultStorage;
		writer = resultStorage.createWriter(config);
	}

	@Override
	public Date getEofDate() {
		return eofDate;
	}

	@Override
	public long getCount() {
		return count;
	}

	@Override
	public boolean isThreadSafe() {
		return true;
	}

	@Override
	public void onRow(Row row) {
		try {
			synchronized (writerLock) {
				writer.write(new Log("$Result$", new Date(), count + 1, row.map()));
			}
		} catch (IOException e) {
			// cancel query when disk is full
			synchronized (writerLock) {
				if (writer.isLowDisk())
					invokeStopCallbacks(QueryStopReason.LowDisk);
			}
			throw new IllegalStateException(e);
		}
		count++;
	}

	private void invokeStopCallbacks(QueryStopReason reason) {
		for (QueryResultCallback c : resultCallbacks) {
			try {
				c.onStop(reason);
			} catch (Throwable t) {
				logger.error("araqne logdb: cannot handle QueryResult.onStop()", t);
			}
		}
	}

	@Override
	public void onRowBatch(RowBatch rowBatch) {
		try {
			synchronized (writerLock) {
				if (rowBatch.selectedInUse) {
					for (int i = 0; i < rowBatch.size; i++) {
						Row row = rowBatch.rows[rowBatch.selected[i]];
						writer.write(new Log("$Result$", new Date(), ++count, row.map()));
					}
				} else {
					for (Row row : rowBatch.rows)
						writer.write(new Log("$Result$", new Date(), ++count, row.map()));
				}
			}
		} catch (IOException e) {
			// cancel query when disk is full
			synchronized (writerLock) {
				if (writer.isLowDisk())
					invokeStopCallbacks(QueryStopReason.LowDisk);
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
			return new LogResultSetImpl(reader, count);
		} catch (Throwable t) {
			if (reader != null)
				reader.close();
			throw new IOException(t);
		}
	}

	@Override
	public void syncWriter() throws IOException {
		synchronized (writerLock) {
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
				writer.close();
			}
		} catch (IOException e) {
		}

		eofDate = new Date();

		for (QueryResultCallback callback : resultCallbacks)
			callback.onPageLoaded(config.getQuery());

		for (QueryStatusCallback callback : config.getQuery().getCallbacks().getStatusCallbacks())
			callback.onChange(config.getQuery());
	}

	@Override
	public void purge() {
		if (purged)
			return;

		purged = true;
		resultCallbacks.clear();

		// delete files
		synchronized (writerLock) {
			writer.purge();
		}
	}

	@Override
	public Set<QueryResultCallback> getResultCallbacks() {
		return resultCallbacks;
	}

	private static class LogResultSetImpl implements QueryResultSet {
		private LogFileReader reader;
		private LogRecordCursor cursor;
		private long count;

		public LogResultSetImpl(LogFileReader reader, long count) throws IOException {
			this.reader = reader;
			this.cursor = reader.getCursor(true);
			this.count = count;
		}

		@Override
		public File getIndexPath() {
			return reader.getIndexPath();
		}

		@Override
		public File getDataPath() {
			return reader.getDataPath();
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
}
