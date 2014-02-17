/*
 * Copyright 2013 Future Systems
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
package org.araqne.logstorage.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.araqne.log.api.LogParserBuilder;
import org.araqne.logstorage.CachedRandomSeeker;
import org.araqne.logstorage.Log;
import org.araqne.logstorage.LogTableRegistry;
import org.araqne.logstorage.file.LogFileReader;

/**
 * not thread-safe
 * 
 * @author xeraph
 * @since 0.9
 */
public class CachedRandomSeekerImpl implements CachedRandomSeeker {
	private boolean closed;
	private LogTableRegistry tableRegistry;
	private LogFileFetcher fetcher;

	private ConcurrentMap<OnlineWriterKey, OnlineWriter> onlineWriters;
	private Map<OnlineWriterKey, List<Log>> onlineBuffers;
	private Map<TabletKey, LogFileReader> cachedReaders;

	public CachedRandomSeekerImpl(LogTableRegistry tableRegistry, LogFileFetcher fetcher,
			ConcurrentMap<OnlineWriterKey, OnlineWriter> onlineWriters) {
		this.tableRegistry = tableRegistry;
		this.fetcher = fetcher;
		this.onlineWriters = onlineWriters;
		this.onlineBuffers = new HashMap<OnlineWriterKey, List<Log>>();
		this.cachedReaders = new HashMap<TabletKey, LogFileReader>();
	}

	private List<Log> getLogsFromOnlineWriter(String tableName, int tableId, Date day, List<Long> ids) {
		OnlineWriterKey onlineKey = new OnlineWriterKey(tableName, day, tableId);
		List<Log> buffer = onlineBuffers.get(onlineKey);
		if (buffer == null) {
			// try load on demand
			OnlineWriter writer = onlineWriters.get(onlineKey);
			if (writer != null) {
				buffer = writer.getBuffer();
				onlineBuffers.put(onlineKey, buffer);
			}
		}

		List<Log> ret = new ArrayList<Log>();
		if (buffer != null) {
			for (Log r : buffer) {
				if (Collections.binarySearch(ids, r.getId(), Collections.reverseOrder()) >= 0) {
					ret.add(r);
				}
			}
		}

		// Because logs in online writer are in ascending order,
		// reverse logs to descending order
		Collections.reverse(ret);
		return ret;
	}

	private LogFileReader getReader(String tableName, int tableId, Date day) throws IOException {
		TabletKey key = new TabletKey(tableId, day);
		LogFileReader reader = cachedReaders.get(key);
		if (reader == null) {
			reader = fetcher.fetch(tableName, day);
			cachedReaders.put(key, reader);
		}
		return reader;
	}

	private List<Long> getFileLogIds(List<Log> onlineLogs, List<Long> ids) {
		int onlineLogCnt = onlineLogs.size();
		int retCnt = ids.size() - onlineLogCnt;
		List<Long> ret = new ArrayList<Long>(retCnt);
		int idx = 0;

		for (long id : ids) {
			// online logs' ids are subset of requested ids.
			if (idx < onlineLogCnt && onlineLogs.get(idx).getId() == id) {
				++idx;
				continue;
			}

			ret.add(id);
		}

		if (ret.size() != retCnt) {
			throw new IllegalStateException("log ids are wrong");
		}

		return ret;
	}

	@Override
	public Log getLog(String tableName, Date day, long id) throws IOException {
		return getLog(tableName, day, id, null);
	}

	@Override
	public void close() {
		if (closed)
			return;

		closed = true;

		for (LogFileReader reader : cachedReaders.values()) {
			reader.close();
		}

		cachedReaders.clear();
	}

	@Override
	public Log getLog(String tableName, Date day, long id, LogParserBuilder builder) {
		List<Log> result = getLogs(tableName, day, null, null, Arrays.asList(new Long[] { id }), builder);
		if (result == null || result.isEmpty())
			return null;
		return result.get(0);
	}

	@Override
	public List<Log> getLogs(String tableName, Date day, Date from, Date to, List<Long> ids, LogParserBuilder builder) {
		if (closed)
			throw new IllegalStateException("already closed");

		int tableId = tableRegistry.getTableId(tableName);

		List<Log> ret = new ArrayList<Log>(ids.size());
		List<Log> onlineLogs = getLogsFromOnlineWriter(tableName, tableId, day, ids);
		List<Long> fileLogIds = getFileLogIds(onlineLogs, ids);
		List<Log> fileLogs = null;

		try {
			LogFileReader reader = getReader(tableName, tableId, day);
			fileLogs = reader.find(from, to, fileLogIds, builder);
		} catch (IOException e) {
		}

		// merge online log and file log
		int i = 0;
		int j = 0;
		for (long id : ids) {
			if (i < onlineLogs.size()) {
				Log l = onlineLogs.get(i);
				if (l.getId() == id) {
					ret.add(l);
					++i;
				}
			} else if (fileLogs != null && j < fileLogs.size()) {
				Log l = fileLogs.get(j);
				if (l.getId() == id) {
					ret.add(l);
					++j;
				}
			}
		}

		return ret;
	}
}
