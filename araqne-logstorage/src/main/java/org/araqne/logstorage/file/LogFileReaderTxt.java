package org.araqne.logstorage.file;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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

	private FilePath indexPath;
	private FilePath dataPath;
	private StorageInputStream indexStream;
	private StorageInputStream dataStream;

	public LogFileReaderTxt(String tableName, FilePath indexPath, FilePath dataPath) throws IOException, InvalidLogFileHeaderException {
			this.tableName = tableName;
			this.indexPath = indexPath;
			this.dataPath = dataPath;

	}

	@Override
	public FilePath getIndexPath() {
		return indexPath;
	}

	@Override
	public FilePath getDataPath() {
		return dataPath;
	}

	@Override
	public void close() {
		ensureClose(indexStream, indexPath);
		ensureClose(dataStream, dataPath);
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
		List<Log> ret = new ArrayList<Log>(ids.size());

		return ret;
	}

	@Override
	public void traverse(Date from, Date to, long minId, long maxId, LogParserBuilder builder, LogTraverseCallback callback,
			boolean doParallel) throws IOException, InterruptedException {

	}

}
