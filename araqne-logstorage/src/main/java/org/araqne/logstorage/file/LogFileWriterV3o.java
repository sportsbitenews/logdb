/*
 * Copyright 2014 Eediom Inc.
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
package org.araqne.logstorage.file;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.atomic.AtomicLong;

import org.araqne.codec.EncodingRule;
import org.araqne.codec.FastEncodingRule;
import org.araqne.logstorage.CallbackSet;
import org.araqne.logstorage.FlushBlockPreprocessingException;
import org.araqne.logstorage.Log;
import org.araqne.logstorage.LogFlushCallback;
import org.araqne.logstorage.LogFlushCallbackArgs;
import org.araqne.logstorage.LogStorageEventArgs;
import org.araqne.logstorage.LogStorageEventListener;
import org.araqne.logstorage.ParaFlushable;
import org.araqne.logstorage.file.Compression;
import org.araqne.logstorage.file.DeflaterCompression;
import org.araqne.logstorage.file.InvalidLogFileHeaderException;
import org.araqne.logstorage.file.LogFileHeader;
import org.araqne.logstorage.file.LogFileWriter;
import org.araqne.logstorage.file.LogRecord;
import org.araqne.logstorage.file.SnappyCompression;
import org.araqne.storage.api.FilePath;
import org.araqne.storage.api.StorageInputStream;
import org.araqne.storage.api.StorageOutputStream;
import org.araqne.storage.filepair.BlockPairWriteCallback;
import org.araqne.storage.filepair.BlockPairWriteCallbackArgs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NOT thread-safe
 * 
 * @author xeraph
 * 
 */
public class LogFileWriterV3o extends LogFileWriter {
	final int BLOCK_LENGTH = 4;
	final int BLOCK_VERSION = 1;
	final int BLOCK_FLAG = 1;
	final int MIN_TIME = 8;
	final int MAX_TIME = 8;
	final int MIN_ID = 8;
	final int MAX_ID = 8;
	final int ORIGINAL_SIZE = 4;
	final int COMPRESS_SIZE = 4;
	final int LENGTH_BLOCK_LENGTH = 4;
	final int FIXED_HEADER_LENGTH = BLOCK_LENGTH + BLOCK_VERSION + BLOCK_FLAG + MIN_TIME + MAX_TIME + MIN_ID + MAX_ID
			+ ORIGINAL_SIZE + COMPRESS_SIZE + LENGTH_BLOCK_LENGTH;

	private Timer timerForCloser;

	private final Logger logger = LoggerFactory.getLogger(LogFileWriterV3o.class);

	private static final short FILE_VERSION = 3;
	private static final int INDEX_HEADER_SIZE = 8 + 8 + 8 + 4;

	private StorageOutputStream indexOutputStream;
	private StorageOutputStream dataOutputStream;
	private long count;
	private AtomicLong lastKey = new AtomicLong();
	private long lastTime;

	private int compressLevel;

	private FilePath indexPath;
	private FilePath dataPath;
	private volatile Date lastFlush = new Date();

	private List<Log> logBuffer;

	private int flags;

	private static class FlushResult {
		public FlushResult(LogFlushCallbackArgs arg) {
			this.args = arg;
		}

		public FlushResult(LogFlushCallbackArgs arg, Throwable t2) {
			this.args = arg;
			this.throwable = t2;
		}

		LogFlushCallbackArgs args;
		Throwable throwable;
	}

	private final int flushCount;

	private String tableName;
	private LogStatsListener listener;
	private String compressionMethod;

	private Set<LogFlushCallback> flushCallbacks;
	private Set<LogStorageEventListener> closeCallbacks;
	private LogFlushCallbackArgs flushCallbackArgs;
	private LogStorageEventArgs closeCallbackArgs;

	private Date day;

	private LogFileHeader indexFileHeader;

	private CallbackSet callbackSet;

	public LogFileWriterV3o(LogWriterConfigV3o config) throws IOException, InvalidLogFileHeaderException {
		this.flushCount = config.getFlushCount();
		this.indexPath = config.getIndexPath();
		this.dataPath = config.getDataPath();
		this.listener = config.getListener();
		this.tableName = config.getTableName();
		this.day = config.getDay();
		this.callbackSet = config.getCallbackSet();
		if (this.callbackSet != null) {
			this.flushCallbacks = callbackSet.get(LogFlushCallback.class);
			this.closeCallbacks = callbackSet.get(LogStorageEventListener.class);
			this.flushCallbackArgs = new LogFlushCallbackArgs(this.tableName);
			this.closeCallbackArgs = new LogStorageEventArgs(this.tableName, this.day);
		}

		// level 0 will not use compression (no zip metadata overhead)
		if (config.getLevel() < 0 || config.getLevel() > 9)
			throw new IllegalArgumentException("compression level should be between 0 and 9");

		logBuffer = new ArrayList<Log>(flushCount);

		boolean indexExists = indexPath.isNotEmpty();
		boolean dataExists = dataPath.isNotEmpty();
		try {

			this.compressLevel = config.getLevel();

			if (indexExists) {
				StorageInputStream indexInputStream = null;
				try {
					indexInputStream = indexPath.newInputStream();

					// get index file header
					indexFileHeader = LogFileHeader.extractHeader(indexInputStream);

					// read last key
					long length = indexInputStream.length();
					long pos = indexFileHeader.size();
					while (pos < length) {
						indexInputStream.seek(pos + 16);
						lastTime = indexInputStream.readLong();
						int logCount = indexInputStream.readInt();
						count += logCount;
						pos += INDEX_HEADER_SIZE;
					}
					lastKey.set(count);

				} finally {
					if (indexInputStream != null) {
						try {
							indexInputStream.close();
						} catch (Throwable t) {
						}
					}
				}

				indexOutputStream = indexPath.newOutputStream(true);
			} else {
				indexFileHeader = new LogFileHeader(FILE_VERSION, LogFileHeader.MAGIC_STRING_INDEX);
				indexOutputStream = indexPath.newOutputStream(false);
				indexOutputStream.write(indexFileHeader.serialize());
			}

			LogFileHeader dataFileHeader = null;
			if (dataExists) {
				StorageInputStream dataInputStream = null;
				try {
					dataInputStream = dataPath.newInputStream();

					// get data file header
					dataFileHeader = LogFileHeader.extractHeader(dataInputStream);

				} finally {
					if (dataInputStream != null) {
						try {
							dataInputStream.close();
						} catch (Throwable t) {
						}
					}
				}

				dataOutputStream = dataPath.newOutputStream(true);
			} else {
				dataFileHeader = new LogFileHeader(FILE_VERSION, LogFileHeader.MAGIC_STRING_DATA);
				byte[] ext = new byte[4];

				prepareInt(flags, ext);
				if (config.getLevel() > 0) {
					byte[] comp = config.getCompression().getBytes();
					ext = new byte[4 + comp.length];
					prepareInt(flags, ext);
					ByteBuffer bb = ByteBuffer.wrap(ext, 4, comp.length);
					bb.put(comp);
				}

				dataFileHeader.setExtraData(ext);
				dataOutputStream = dataPath.newOutputStream(false);
				dataOutputStream.write(dataFileHeader.serialize());
			}

			// select compression method
			byte[] ext = dataFileHeader.getExtraData();
			compressionMethod = new String(ext, 4, ext.length - 4);

		} catch (Throwable t) {
			ensureClose();
			throw new IllegalStateException(t);
		}
	}

	/**
	 * @since 2.9.0 (w/ araqne-logstorage 2.5.0)
	 */
	@Override
	public boolean isLowDisk() {
		FilePath dir = indexPath.getAbsoluteFilePath().getParentFilePath();
		return dir != null && dir.getUsableSpace() == 0;
	}

	@Override
	public long getLastKey() {
		return lastKey.get();
	}

	@Override
	public Date getLastDate() {
		return new Date(lastTime);
	}

	@Override
	public long getCount() {
		return count;
	}

	@Override
	public void write(Log log) throws IOException {
		if (closeCompleted)
			throw new IllegalStateException("already closed.");

		// do not remove this condition (date.toString() takes many CPU time)
		if (logger.isDebugEnabled())
			logger.debug(
					"araqne logstorage: write new log, idx [{}], dat [{}], id {}, time {}",
					new Object[] { indexPath.getAbsolutePath(), dataPath.getAbsolutePath(), log.getId(),
							log.getDate().toString() });

		// check validity
		long newKey = log.getId();
		if (newKey <= lastKey.get())
			throw new IllegalArgumentException("invalid key: " + newKey + ", last key was " + lastKey);

		logBuffer.add(log);

		if (logBuffer.size() == flushCount)
			flush(false);

		// update last key
		lastKey.set(newKey);
		long time = log.getDate().getTime();
		lastTime = (lastTime < time) ? time : lastTime;

		count++;
	}

	private void invokeFlushCompletionCallback(FlushResult flushResult) {
		for (LogFlushCallback callback : flushCallbacks) {
			try {
				if (flushResult.throwable == null)
					callback.onFlushCompleted(flushResult.args);
				else
					callback.onFlushException(flushResult.args, flushResult.throwable);
			} catch (Throwable t) {
				logger.warn("flush callback should not throw any exception", t);
			}
		}
	}

	@Override
	public void write(List<Log> data) throws IOException {
		if (data.size() == 0)
			return;

		if (closeCompleted)
			throw new IllegalStateException("already closed.");

		Log firstLog = data.get(0);
		Log lastLog = data.get(data.size() - 1);

		// check validity
		long newKey = firstLog.getId();
		if (newKey <= lastKey.get())
			throw new IllegalArgumentException("invalid key: " + newKey + ", last key was " + lastKey);

		logBuffer.addAll(data);

		if (logBuffer.size() >= flushCount)
			flush(false);

		// update last key
		lastKey.set(lastLog.getId());

		for (Log log : data) {
			long time = log.getDate().getTime();
			lastTime = (lastTime < time) ? time : lastTime;
		}

		count += data.size();
	}

	@Override
	public List<Log> getBuffer() {
		List<Log> captured = logBuffer;
		int capturedLimit = captured.size();
		List<Log> result = new ArrayList<Log>(capturedLimit);
		for (int i = 0; i < capturedLimit; ++i) {
			result.add(captured.get(i));
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<List<Log>> getBuffers() {
		// assumes lock is hold by OnlineWriter.
		return Arrays.asList(logBuffer);
	}

	private int flushBlockConstructionCount = 0;

	private volatile boolean closeCompleted = false;

	private static class LogFileSegment {
		public long blockMinTime;
		public long blockMaxTime;
		public long minId;
		public long maxId;
		public int dataOriSize;
		public int compressedSize;
		public int[] lengthArray;
		public ByteBuffer compressed;
		public byte[] iv = new byte[16];
		public byte[] signature;
		public byte blockFlag = 0;

		// non-physical
		public int lengthBlockSize;
		private ByteBuffer dataBuffer;
		public int blockLogCount;

		public LogFileSegment(int blockSize, int lengthBlockSize, int[] lengthArray) {
			this.dataBuffer = ByteBuffer.allocate(blockSize);
			this.lengthBlockSize = lengthBlockSize;
			this.lengthArray = lengthArray;
			this.compressed = null;
		}
	}

	private Compression newCompression() {
		if (compressionMethod.equals("snappy"))
			return new SnappyCompression();
		else
			return new DeflaterCompression(compressLevel); // "deflater" or "deflate"
	}

	public class ParaSlotItem implements ParaFlushable {
		private final List<Log> logBuffer;
		private LogFileSegment segment;
		private boolean ready = false;
		private Compression comp;
		private final LogFileWriterV3o writer;
		private int serialNumber;
		private long minId;
		private long maxId;

		public ParaSlotItem(LogFileWriterV3o w, List<Log> logBuffer) {
			this.writer = w;
			this.logBuffer = logBuffer;
			this.serialNumber = flushBlockConstructionCount++;
			this.comp = newCompression();
			this.minId = logBuffer.get(0).getId();
			this.maxId = logBuffer.get(logBuffer.size() - 1).getId();
		}

		@Override
		public String toString() {
			return "ParaSlotItem [ready=" + ready + ", serialNumber=" + serialNumber + ", minId=" + minId + ", maxId="
					+ maxId
					+ ", segment=" + segment + "]";
		}

		@Override
		public boolean isReady() {
			return ready;
		}

		public void preprocess() {
			try {
				this.segment = buildSegment();
				LogFileSegment seg = this.segment;
				seg.dataBuffer.flip();
				seg.dataOriSize = seg.dataBuffer.remaining();

				// XXX: select either original or deflated by deflated size
				if (compressLevel > 0) {
					seg.compressed = comp.compress(seg.dataBuffer.array(), 0, seg.dataBuffer.limit());
					seg.compressedSize = seg.compressed.limit();
					if (seg.compressedSize >= seg.dataBuffer.limit()) {
						seg.compressed = seg.dataBuffer;
						seg.compressedSize = seg.dataBuffer.limit();
						seg.blockFlag |= 0x20;
					}
				} else {
					seg.compressed = seg.dataBuffer;
					seg.compressedSize = seg.compressed.limit();
				}

				seg.dataBuffer = null;
				this.ready = true;
			} catch (Throwable t) {
				throw new FlushBlockPreprocessingException(this, "unexpected exception", t);
			}
		}

		private int getByteCount(LogRecord record) {
			final int DATE_LEN = 8;
			final int LENGTH_LEN = 4;
			return DATE_LEN + LENGTH_LEN + record.getData().remaining();
		}

		private LogFileSegment buildSegment() {
			byte[] intbuf = new byte[4];
			byte[] longbuf = new byte[8];

			LogRecord[] records = new LogRecord[logBuffer.size()];
			int[] lengthArray = new int[records.length];

			int i = 0;
			int lengthBlockSize = 0;
			for (Log data : logBuffer) {
				LogRecord record = convert(data);
				records[i] = record;
				int len = getByteCount(record);
				lengthArray[i] = len;
				lengthBlockSize += EncodingRule.lengthOfRawNumber(int.class, len);
				i++;
			}

			int segmentSize = calculateBufferSize(records, lengthBlockSize);
			LogFileSegment seg = new LogFileSegment(segmentSize, lengthBlockSize, lengthArray);
			seg.blockLogCount = records.length;

			long minTime = Long.MAX_VALUE;
			long maxTime = Long.MIN_VALUE;
			for (LogRecord record : records) {
				long time = record.getDate().getTime();
				minTime = (minTime <= time) ? minTime : time;
				maxTime = (maxTime >= time) ? maxTime : time;

				prepareLong(record.getDate().getTime(), longbuf);
				seg.dataBuffer.put(longbuf);
				prepareInt(record.getData().remaining(), intbuf);
				seg.dataBuffer.put(intbuf);
				ByteBuffer b = record.getData();
				if (b.remaining() == b.array().length) {
					seg.dataBuffer.put(b.array());
				} else {
					byte[] array = new byte[b.remaining()];
					int pos = b.position();
					b.get(array);
					b.position(pos);
					seg.dataBuffer.put(array);
				}
			}

			seg.blockMinTime = minTime;
			seg.blockMaxTime = maxTime;
			seg.minId = records[0].getId();
			seg.maxId = records[records.length - 1].getId();

			return seg;
		}

		private int calculateBufferSize(LogRecord[] records, int lengthBlockSize) {

			int dataLength = 0;
			for (int i = 0; i < records.length; i++) {
				dataLength += getByteCount(records[i]);
			}

			int size = FIXED_HEADER_LENGTH + lengthBlockSize + dataLength;

			return size;
		}

		private LogRecord convert(Log log) {
			ByteBuffer bb = new FastEncodingRule().encode(log.getData());
			LogRecord logdata = new LogRecord(log.getDate(), log.getId(), bb);
			log.setBinaryLength(bb.remaining());
			return logdata;
		}

		@Override
		public void flush() throws IOException {
			try {
				// already closed. (may be dropped)
				if (dataOutputStream == null && indexOutputStream == null) {
					logger.info("table already closed (may be dropped)");
					return;
				}

				long ipos = indexOutputStream.getPos();
				long dpos = dataOutputStream.getPos();

				ByteArrayOutputStream dmStream = new ByteArrayOutputStream();
				ByteArrayOutputStream imStream = new ByteArrayOutputStream();

				LogFileSegment seg = this.segment;
				// using "this" in front of each instance variable
				// to make it explicit that using block's variable.
				byte[] longbuf = new byte[8];
				byte[] intbuf = new byte[4];

				// write data file pointer to index header
				prepareLong(dataOutputStream.getPos(), longbuf);
				imStream.write(longbuf);

				// write block length, version, flag

				int blockSize = FIXED_HEADER_LENGTH + seg.lengthBlockSize + seg.compressedSize;

				prepareInt(blockSize, intbuf);
				dmStream.write(intbuf);
				dmStream.write(3);
				dmStream.write(seg.blockFlag);

				// write min datetime
				prepareLong(seg.blockMinTime, longbuf);
				dmStream.write(longbuf);
				imStream.write(longbuf);

				// write max datetime
				prepareLong(seg.blockMaxTime, longbuf);
				dmStream.write(longbuf);
				imStream.write(longbuf);

				// write min id
				prepareLong(seg.minId, longbuf);
				dmStream.write(longbuf);

				// write max id
				prepareLong(seg.maxId, longbuf);
				dmStream.write(longbuf);

				// write original size
				prepareInt(seg.dataOriSize, intbuf);
				dmStream.write(intbuf);

				// write compressed size
				prepareInt(seg.compressedSize, intbuf);
				dmStream.write(intbuf);

				// write length block length
				prepareInt(seg.lengthBlockSize, intbuf);
				dmStream.write(intbuf);

				// write length block
				ByteBuffer lengthBlock = ByteBuffer.allocate(seg.lengthBlockSize);
				for (long l : seg.lengthArray)
					EncodingRule.encodeRawNumber(lengthBlock, long.class, l);

				dmStream.write(lengthBlock.array());

				// write cipher extension
				if ((seg.blockFlag & 0x80) == 0x80) {
					dmStream.write(seg.iv);
					dmStream.write(seg.signature);
				}

				// write compressed logs
				dmStream.write(seg.compressed.array(), 0, seg.compressed.remaining());

				// dataFile.getFD().sync();
				dataOutputStream.write(dmStream.toByteArray());

				// write log count
				prepareInt(seg.blockLogCount, intbuf);
				imStream.write(intbuf);

				indexOutputStream.write(imStream.toByteArray());

				// call blockpair callback
				if (callbackSet != null) {
					for (BlockPairWriteCallback cb : callbackSet.get(BlockPairWriteCallback.class)) {
						try {
							cb.onWriteCompleted(new BlockPairWriteCallbackArgs(
									"v3o", LogFileWriterV3o.this.tableName, LogFileWriterV3o.this.day,
									(int) ((ipos - indexFileHeader.size()) / INDEX_HEADER_SIZE),
									ipos, imStream.toByteArray(), dpos, dmStream.toByteArray()));
						} catch (Throwable t) {
							logger.warn("BlockPairWriteCallback should not throw an exception", t);
						}
					}
				}

				// invoke stats callback
				if (listener != null) {
					LogStats stats = new LogStats();
					stats.setTableName(tableName);
					stats.setLogCount((int) (seg.maxId - seg.minId + 1));
					stats.setBlockSize(blockSize);
					stats.setOriginalDataSize(seg.dataOriSize);
					stats.setCompressedDataSize(seg.compressedSize);

					listener.onWrite(stats);
				}
			} finally {
			}
		}

		@Override
		public int getSerialNumber() {
			return serialNumber;
		}

		public LogFileWriterV3o getWriter() {
			return writer;
		}
	}

	@Override
	public boolean flush(boolean sweep) throws IOException {
		// assumes lock is hold by OnlineWriter.

		// check if writer is closed
		if (indexOutputStream == null || dataOutputStream == null)
			return false;

		if (logger.isTraceEnabled())
			logger.trace("araqne logstorage: flush idx [{}], dat [{}] files", indexPath, dataPath);

		// mark last flush
		lastFlush = new Date();

		if (logBuffer.size() != 0) {
			final ParaSlotItem block = new ParaSlotItem(this, clearLogQueue());
			invokeOnFlush(block);
			block.preprocess();
			block.ready = true;
			try {
				block.flush();
				if (flushCallbackArgs != null)
					invokeFlushCompletionCallback(newLogFlushCallbackArgs(block.logBuffer));
			} catch (Exception e) {
				if (flushCallbackArgs != null)
					invokeFlushCompletionCallback(newLogFlushCallbackArgs(block, e));
			}
		}
		return true;
	}

	private void invokeOnFlush(ParaSlotItem block) {
		if (flushCallbacks != null)
			for (LogFlushCallback callback : flushCallbacks) {
				try {
					LogFlushCallbackArgs arg = flushCallbackArgs.shallowCopy();
					arg.setLogs(block.logBuffer);
					callback.onFlush(arg);
				} catch (Throwable t) {
					logger.warn(this + "{}: exception in log flush callback", t);
				}
			}
	}

	private FlushResult newLogFlushCallbackArgs(List<Log> logBuffer) {
		LogFlushCallbackArgs arg = flushCallbackArgs.shallowCopy();
		arg.setLogs(logBuffer);
		return new FlushResult(arg);
	}

	private FlushResult newLogFlushCallbackArgs(ParaSlotItem item, Throwable t) {
		LogFlushCallbackArgs arg = flushCallbackArgs.shallowCopy();
		if (item == null) {
			arg.setLogs(null);
		} else {
			arg.setLogs(item.logBuffer);
		}
		return new FlushResult(arg, t);
	}

	private List<Log> clearLogQueue() {
		List<Log> oldBuffer = logBuffer;
		logBuffer = new ArrayList<Log>(flushCount);

		return oldBuffer;
	}

	@Override
	public void sync() throws IOException {
		if (indexOutputStream == null || dataOutputStream == null)
			return;

		if (closeCompleted)
			return;

		dataOutputStream.sync();
		indexOutputStream.sync();
	}

	static void prepareInt(int l, byte[] b) {
		for (int i = 0; i < 4; i++)
			b[i] = (byte) ((l >> ((3 - i) * 8)) & 0xff);
	}

	static void prepareLong(long l, byte[] b) {
		for (int i = 0; i < 8; i++)
			b[i] = (byte) ((l >> ((7 - i) * 8)) & 0xff);
	}

	public Date getLastFlush() {
		return lastFlush;
	}

	@Override
	public void close() throws IOException {
		if (!closeCompleted) {
			try {
				logger.debug("{}: closing, lastKey: {}", this, lastKey);
				flush(false);
				ensureClose();
			} catch (IOException e) {
				logger.warn("IOException while last flush before closing. DATA MAY BE DROPPED.", e);
			}
		}
	}

	public void ensureClose() {
		logger.debug("eusureClose called: {}", this.indexPath);

		if (indexOutputStream != null) {
			try {
				indexOutputStream.close();
			} catch (IOException e) {
				logger.warn("exception while closing", e);
			}
			indexOutputStream = null;
		}
		if (dataOutputStream != null) {
			try {
				dataOutputStream.close();
			} catch (IOException e) {
				logger.warn("exception while closing", e);
			}
			dataOutputStream = null;
		}
		if (timerForCloser != null)
			timerForCloser.cancel();

		if (logger.isDebugEnabled())
			logger.debug("log file writer closed: {}", this.indexPath);

		closeCompleted = true;

		invokeCloseCallback();
	}

	private void invokeCloseCallback() {
		if (closeCallbacks != null && closeCallbackArgs != null) {
			for (LogStorageEventListener callback : closeCallbacks) {
				callback.onClose(closeCallbackArgs.tableName, closeCallbackArgs.day);
			}
		}
	}

	@Override
	public boolean isClosed() {
		// change order: cf.get().closed -> closeCompleted
		// To check if closed without locking, checking closeCompleted first.
		return closeCompleted;
	}

	/**
	 * @since 2.9.0 (w/ araqne-logstorage 2.5.0)
	 */
	@Override
	public void purge() {
		boolean result = indexPath.delete();
		logger.debug("logpresso logstorage: delete [{}] file => {}", indexPath, result);

		result = dataPath.delete();
		logger.debug("logpresso logstorage: delete [{}] file => {}", dataPath, result);
	}

	public String toString() {
		FilePath parentDir = indexPath.getAbsoluteFilePath().getParentFilePath();
		String parentDirStr = null;
		if (parentDir == null)
			parentDirStr = "null";
		else
			parentDirStr = parentDir.getName();
		return "LogFileWriterV3: " + parentDirStr + "/" + indexPath.getName();
	}
}
