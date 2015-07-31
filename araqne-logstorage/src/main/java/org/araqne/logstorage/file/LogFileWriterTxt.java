package org.araqne.logstorage.file;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.araqne.logstorage.CallbackSet;
import org.araqne.logstorage.Log;
import org.araqne.storage.api.FilePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogFileWriterTxt extends LogFileWriter {
	private final Logger logger = LoggerFactory.getLogger(LogFileWriterTxt.class.getName());
	private boolean closed = false;
	private volatile Date lastFlush = new Date();

	public LogFileWriterTxt(FilePath indexPath, FilePath dataPath, CallbackSet cbSet, String tableName, Date day,
			AtomicLong lastKey2) throws Throwable {

	}

	@Override
	public boolean isLowDisk() {
		return false;
	}

	@Override
	public long getLastKey() {
		return 0;
	}

	@Override
	public Date getLastDate() {
		return null;
	}

	@Override
	public long getCount() {
		return 0;
	}

	@Override
	public void write(Log log) throws IOException {
		logger.error("Txt file service does not support write function");
		throw new UnsupportedOperationException("Cannot make new writer - txt file service");
	}

	@Override
	public void write(List<Log> data) throws IOException {
		logger.error("Txt file service does not support write function");
		throw new UnsupportedOperationException("Cannot make new writer - txt file service");
	}

	@Override
	public List<Log> getBuffer() {
		return null;
	}

	@Override
	public List<List<Log>> getBuffers() {
		return null;
	}

	@Override
	public boolean flush(boolean sweep) throws IOException {
		lastFlush = new Date();
		return true;
	}

	@Override
	public void sync() throws IOException {
	}

	@Override
	public Date getLastFlush() {
		return lastFlush;
	}

	@Override
	public void close() throws IOException {
		closed = true;
	}

	@Override
	public boolean isClosed() {
		return closed;
	}

	/**
	 * @since 2.5.0
	 */
	@Override
	public void purge() {
	}

	@Override
	public void setCallbackSet(CallbackSet callbackSet) {
	}

	@Override
	public CallbackSet getCallbackSet() {
		return null;
	}
}
