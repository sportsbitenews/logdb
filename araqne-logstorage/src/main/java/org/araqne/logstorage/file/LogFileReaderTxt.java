package org.araqne.logstorage.file;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.TreeMap;

import org.araqne.log.api.LogParser;
import org.araqne.log.api.LogParserBugException;
import org.araqne.log.api.LogParserBuilder;
import org.araqne.logstorage.Log;
import org.araqne.logstorage.LogTraverseCallback;
import org.araqne.storage.api.FilePath;
import org.araqne.storage.api.StorageInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogFileReaderTxt extends LogFileReader {
	private Logger logger = LoggerFactory.getLogger(LogFileReaderTxt.class);
	private String tableName;

	private List<FilePath> dataPathList;

	private Map<FilePath, StorageInputStream> dataStreamMap;

	private Date date;

	public LogFileReaderTxt(String tableName, List<FilePath> dataPathList, Date date) throws IOException, InvalidLogFileHeaderException {
		this.tableName = tableName;
		this.dataPathList = dataPathList;
		
		dataStreamMap = new TreeMap<FilePath, StorageInputStream>();
		ListIterator<FilePath> dataPathListIterator = dataPathList.listIterator();
		while (dataPathListIterator.hasNext()) {
			FilePath datapath = (FilePath) dataPathListIterator.next();
			dataStreamMap.put(datapath, datapath.newInputStream());
		}

		this.date = date;
	}

	@Override
	public FilePath getIndexPath() {
		return null;
	}

	@Override
	public FilePath getDataPath() {
		return dataPathList.get(0);
	}

	@Override
	public void close() {
		ListIterator<FilePath> dataPathIterator = dataPathList.listIterator();
		while(dataPathIterator.hasNext()){
			FilePath dataPath = dataPathIterator.next();
			StorageInputStream dataStream = dataStreamMap.get(dataPath);
			ensureClose(dataStream, dataPath);
		}
	}

	private void ensureClose(Closeable stream, FilePath path) {
		try {
			if (stream != null)
				stream.close();
		} catch (IOException e) {
			logger.error("araqne logstorage: cannot close file - " + path.getAbsolutePath(), e);
		}
	}

	@Override
	public LogRecordCursor getCursor() throws IOException {
		return getCursor(false);
	}

	@Override
	public LogRecordCursor getCursor(boolean ascending) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public LogBlockCursor getBlockCursor() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<Log> find(Date from, Date to, List<Long> ids, LogParserBuilder builder) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void traverse(Date from, Date to, long minId, long maxId, LogParserBuilder builder, LogTraverseCallback callback,
			boolean doParallel) throws IOException, InterruptedException {
		boolean suppressBugAlert = false;
		LogParser parser = null;
		if (builder != null)
			parser = builder.build();

		int _id = 1;
		List<Log> logs = new ArrayList<Log>();

		ListIterator<FilePath> dataPathIterator = dataPathList.listIterator();
		while(dataPathIterator.hasNext()){
			FilePath dataPath = dataPathIterator.next();
			StorageInputStream dataStream = dataStreamMap.get(dataPath);

			String fileName = dataPath.getAbsolutePath();

			String line = null;
			while ((line = dataStream.readLine()) != null) {
				List<Log> result = null;

				Map<String, Object> data = new HashMap<String, Object>();
				data.put("_fileName", fileName);
				data.put("line", line);

				Log log = new Log(tableName, date, _id++, data);

				try {
					result = parse(tableName, parser, log);
				} catch (LogParserBugException e) {
					result = new ArrayList<Log>(1);
					result.add(new Log(e.tableName, e.date, e.id, e.logMap));
					if (!suppressBugAlert) {
						logger.error("araqne logstorage: PARSER BUG! original log => table " + e.tableName + ", id " + e.id
								+ ", data " + e.logMap, e.cause);
						suppressBugAlert = true;
					}
				} finally {
					if (result == null)
						continue;

					logs.addAll(result);
				}
			}
		}
		callback.writeLogs(logs);
	}

}
