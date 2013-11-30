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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.araqne.codec.EncodingRule;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.Row;
import org.araqne.logdb.Query;
import org.araqne.logdb.QueryResultSet;
import org.araqne.logdb.QueryResult;
import org.araqne.logdb.QueryResultCallback;
import org.araqne.logdb.QueryStatusCallback;
import org.araqne.logdb.RowBatch;
import org.araqne.logstorage.Log;
import org.araqne.logstorage.file.LogFileReaderV2;
import org.araqne.logstorage.file.LogFileWriterV2;
import org.araqne.logstorage.file.LogRecord;
import org.araqne.logstorage.file.LogRecordCursor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryResultV2 implements QueryResult {
	private final Logger logger = LoggerFactory.getLogger(QueryResultV2.class);
	private static File BASE_DIR = new File(System.getProperty("araqne.data.dir"), "araqne-logdb/query/");
	private LogFileWriterV2 writer;
	private File indexPath;
	private File dataPath;
	private long count;
	private Query query;
	/**
	 * index and data file is deleted by user request
	 */
	private volatile boolean purged;

	private volatile boolean writerClosed;
	private volatile Date eofDate;

	public QueryResultV2(Query query) throws IOException {
		this("", query);
	}

	public QueryResultV2(String tag, Query query) throws IOException {
		this.query = query;

		BASE_DIR.mkdirs();

		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd_HHmmss");
		if (tag == null || tag.isEmpty())
			tag = query.getId() + "-" + df.format(new Date());

		indexPath = new File(BASE_DIR, "result-" + tag + ".idx");
		dataPath = new File(BASE_DIR, "result-" + tag + ".dat");
		writer = new LogFileWriterV2(indexPath, dataPath, 1024 * 1024, 1);
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
	public void onRow(Row row) {
		try {
			synchronized (writer) {
				writer.write(new Log("$Result$", new Date(), count + 1, row.map()));
			}
		} catch (IOException e) {
			// cancel query when disk is full
			File dir = indexPath.getParentFile();
			if (dir != null && dir.getFreeSpace() == 0)
				query.stop(QueryStopReason.LowDisk);

			throw new IllegalStateException(e);
		}
		count++;
	}

	@Override
	public void onRowBatch(RowBatch rowBatch) {
		try {
			synchronized (writer) {
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
			File dir = indexPath.getParentFile();
			if (dir != null && dir.getFreeSpace() == 0)
				query.stop(QueryStopReason.LowDisk);

			throw new IllegalStateException(e);
		}
	}

	public QueryResultSet getResult() throws IOException {
		if (purged) {
			String msg = "query result file is already purged, index=" + indexPath.getAbsolutePath() + ", data="
					+ dataPath.getAbsolutePath();
			throw new IOException(msg);
		}

		syncWriter();

		// TODO : check tableName
		LogFileReaderV2 reader = null;
		try {
			reader = new LogFileReaderV2(null, indexPath, dataPath);
			return new LogResultSetImpl(reader, count);
		} catch (Throwable t) {
			if (reader != null)
				reader.close();
			throw new IOException(t);
		}
	}

	@Override
	public void syncWriter() throws IOException {
		synchronized (writer) {
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
			synchronized (writer) {
				writer.close();
			}
		} catch (IOException e) {
		}

		eofDate = new Date();

		for (QueryResultCallback callback : query.getCallbacks().getResultCallbacks())
			callback.onPageLoaded(query);

		for (QueryStatusCallback callback : query.getCallbacks().getStatusCallbacks())
			callback.onChange(query);
	}

	@Override
	public void purge() {
		purged = true;

		// delete files
		boolean result = indexPath.delete();
		logger.debug("araqne logdb: delete result .idx file [{}] => {}", indexPath, result);
		result = dataPath.delete();
		logger.debug("araqne logdb: delete result .dat file [{}] => {}", dataPath, result);
	}

	private static class LogResultSetImpl implements QueryResultSet {
		private LogFileReaderV2 reader;
		private LogRecordCursor cursor;
		private long count;

		public LogResultSetImpl(LogFileReaderV2 reader, long count) throws IOException {
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
