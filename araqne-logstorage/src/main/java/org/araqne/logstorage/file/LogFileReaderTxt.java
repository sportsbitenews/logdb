package org.araqne.logstorage.file;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
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
import org.araqne.logstorage.TableScanRequest;
import org.araqne.storage.api.FilePath;
import org.araqne.storage.api.StorageInputStream;
import org.araqne.storage.api.StorageUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogFileReaderTxt extends LogFileReader {
	private Logger logger = LoggerFactory.getLogger(LogFileReaderTxt.class);
	private String tableName;

	private List<FilePath> dataPathList;
	private Map<FilePath, StorageInputStream> dataStreamMap;
	private String charset;

	private Date date;

	public LogFileReaderTxt(String tableName, List<FilePath> dataPathList, Date date, String charset) throws IOException, InvalidLogFileHeaderException {
		this.tableName = tableName;
		this.dataPathList = dataPathList;
		
		dataStreamMap = new TreeMap<FilePath, StorageInputStream>();
		ListIterator<FilePath> dataPathListIterator = dataPathList.listIterator();
		while (dataPathListIterator.hasNext()) {
			FilePath datapath = (FilePath) dataPathListIterator.next();
			dataStreamMap.put(datapath, datapath.newInputStream());
		}

		this.date = date;
		this.charset = charset;
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
	public void traverse(TableScanRequest req) throws IOException, InterruptedException {
		boolean suppressBugAlert = false;
		LogParser parser = null;
		if (req.getParserBuilder() != null)
			parser = req.getParserBuilder().build();

		int id = 1;

		ListIterator<FilePath> dataPathIterator = dataPathList.listIterator();
		while (dataPathIterator.hasNext()){
			FilePath dataPath = dataPathIterator.next();
			StorageInputStream dataStream = dataStreamMap.get(dataPath);

			String fileName = dataPath.getAbsolutePath();
			InputStreamReader dataStreamReader = null;
			BufferedReader dataStreamBufferedReader = null;
			try {
				dataStreamReader = new InputStreamReader(dataStream, charset);
				dataStreamBufferedReader = new BufferedReader(dataStreamReader);

				String line = null;
				LogTraverseCallback callback = req.getTraverseCallback();
				while (!callback.isEof() && (line = dataStreamBufferedReader.readLine()) != null) {
					List<Log> result = null;

					Map<String, Object> data = new HashMap<String, Object>();
					data.put("_file", fileName);
					data.put("line", line);

					Log log = new Log(tableName, date, id++, data);

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

						callback.writeLogs(result);
					}
				}
			} finally {
				StorageUtil.ensureClose(dataStreamBufferedReader);
				StorageUtil.ensureClose(dataStreamReader);
			}
		}
	}

}
