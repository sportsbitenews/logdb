/*
 * Copyright 2013 Eediom Inc. All rights reserved.
 */
package org.araqne.logstorage.file;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.araqne.log.api.LogParser;
import org.araqne.log.api.LogParserBugException;
import org.araqne.log.api.LogParserBuilder;
import org.araqne.logstorage.Crypto;
import org.araqne.logstorage.Log;
import org.araqne.logstorage.LogMarshaler;
import org.araqne.logstorage.LogTraverseCallback;
import org.araqne.logstorage.TableScanRequest;
import org.araqne.storage.api.FilePath;
import org.araqne.storage.api.StorageInputStream;
import org.araqne.storage.crypto.LogCryptoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogFileReaderV3o extends LogFileReader {
	private final Logger logger = LoggerFactory.getLogger(LogFileReaderV3o.class);
	private static final int FILE_VERSION = 3;

	private FilePath indexPath;
	private FilePath dataPath;
	private StorageInputStream indexStream;
	private StorageInputStream dataStream;

	private List<IndexBlockV3Header> indexBlockHeaders = new ArrayList<IndexBlockV3Header>();

	// current loaded buffer
	DataBlockV3 cachedBlock = null;

	private String compressionMethod;

	private long totalCount;
	private String tableName;

	private Date day;
	private LogCryptoService cryptoService;

	public LogFileReaderV3o(LogReaderConfigV3o c) throws IOException, InvalidLogFileHeaderException {
		// this.cache = c.cache;
		this.day = c.day;

		try {
			this.tableName = c.tableName;
			this.indexPath = c.indexPath;
			this.dataPath = c.dataPath;
			this.cryptoService = c.cryptoService;

			loadIndexFile();
			loadDataFile();

		} catch (Throwable t) {
			ensureClose(indexStream, indexPath);
			ensureClose(dataStream, dataPath);
			throw new IllegalStateException("cannot open log file reader v3: index=" + indexPath.getAbsolutePath() + ", data="
					+ dataPath.getAbsolutePath(), t);
		}
	}

	@Override
	public FilePath getIndexPath() {
		return indexPath;
	}

	@Override
	public FilePath getDataPath() {
		return dataPath;
	}
	
	private void loadIndexFile() throws IOException, InvalidLogFileHeaderException {
		BufferedInputStream indexReader = null;
		try {
			this.indexStream = indexPath.newInputStream();
			LogFileHeader indexFileHeader = LogFileHeader.extractHeader(indexStream);
			if (indexFileHeader.version() != FILE_VERSION)
				throw new InvalidLogFileHeaderException("version not match, index file " + indexPath.getAbsolutePath());

			long length = indexStream.length();
			long pos = indexFileHeader.size();

			indexReader = new BufferedInputStream(indexPath.newInputStream());
			indexReader.skip(pos);

			IndexBlockV3Header unserializer = new IndexBlockV3Header();
			int id = 0;
			while (pos < length) {
				IndexBlockV3Header header = null;
				try {
					header = unserializer.unserialize(id++, indexReader);
				} catch (IOException e) {
					break;
				}
				
				header.firstId = totalCount + 1;
				header.fp = pos;
				header.ascLogCount = totalCount;
				
				// skip reserved blocks
				if (!header.isReserved()) {
					totalCount += header.logCount;
					indexBlockHeaders.add(header);
				}
				
				pos += IndexBlockV3Header.ITEM_SIZE;
			}

			long t = 0;
			for (int i = indexBlockHeaders.size() - 1; i >= 0; i--) {
				IndexBlockV3Header h = indexBlockHeaders.get(i);
				h.dscLogCount = t;
				t += h.logCount;
			}

			logger.trace("araqne logstorage: {} has {} blocks (total {} blocks), {} logs.",
					new Object[] { indexPath.getName(), indexBlockHeaders.size(), id, totalCount });
		} finally {
			if (indexReader != null)
				indexReader.close();
		}
	}

	private void loadDataFile() throws IOException, InvalidLogFileHeaderException {
		this.dataStream = dataPath.newInputStream();
		LogFileHeader dataFileHeader = LogFileHeader.extractHeader(dataStream);
		if (dataFileHeader.version() != FILE_VERSION)
			throw new InvalidLogFileHeaderException("version not match, data file");

		byte[] ext = dataFileHeader.getExtraData();

		compressionMethod = new String(ext, 4, ext.length - 4).trim();
		if (compressionMethod.length() == 0)
			compressionMethod = null;

		logger.debug("logpresso logstorage: file [{}] compression [{}]", dataPath.getAbsolutePath(), compressionMethod);
	}

	public String toString() {
		return "LogFileReaderV3 [tableName=" + tableName + ", day=" + day + "]";
	}

	private synchronized DataBlockV3 loadDataBlock(IndexBlockV3Header index, StorageInputStream dataStream) throws IOException {
		DataBlockV3Params p = new DataBlockV3Params();
		p.indexHeader = index;
		p.dataStream = dataStream;
		p.dataPath = dataPath;
		p.compressionMethod = compressionMethod;

		// update local cache
		if (cachedBlock == null || cachedBlock.getDataFp() != index.dataFp) {
			cachedBlock = new DataBlockV3(p);
		}

		if (cachedBlock.isBroken())
			return null;

		// use local cache
		return cachedBlock;
	}

	private class ReadBlockRequest {
		IndexBlockV3Header header;
		List<Long> ids;

		public ReadBlockRequest(IndexBlockV3Header header, List<Long> ids) {
			this.header = header;
			this.ids = ids;
		}

		@Override
		public String toString() {
			return "ReadBlockRequest [header=" + header + ", ids.size()=" + ids.size() + "]";
		}
	}

	private List<ReadBlockRequest> makeRequests(List<Long> filteredIDs) {
		// ids should be filtered and in descending order
		List<Long> ids = filteredIDs;
		if (ids == null || ids.isEmpty())
			return null;

		long maxID = ids.get(0);
		long minID = ids.get(ids.size() - 1);

		int l = 0;
		int r = indexBlockHeaders.size() - 1;
		while (r >= l) {
			int m = (l + r) / 2;
			IndexBlockV3Header header = indexBlockHeaders.get(m);

			long blockMin = header.firstId;
			long blockMax = blockMin + header.logCount;

			if (maxID >= blockMin && minID < blockMax) {
				return makeRequestsWithMatchedHeader(header, ids);
			} else if (maxID < blockMin) {
				r = m - 1;
			} else {
				l = m + 1;
			}
		}

		return null;
	}

	// cut id list by [min, max) range
	private List<List<Long>> cutRange(List<Long> ids, long min, long max) {
		List<List<Long>> ret = new ArrayList<List<Long>>();
		ret.add(new ArrayList<Long>());
		final int UPPER_RANGE = 2;
		final int INSIDE_RANGE = 1;
		final int LOWER_RANGE = 0;

		int state = UPPER_RANGE;
		for (long id : ids) {
			if ((state == UPPER_RANGE && id < max) || (state == INSIDE_RANGE && id < min)) {
				if (!ret.get(ret.size() - 1).isEmpty()) {
					ret.add(new ArrayList<Long>());
				}
				state = (id < min) ? LOWER_RANGE : INSIDE_RANGE;
			}

			ret.get(ret.size() - 1).add(id);
		}
		return ret;
	}

	private List<ReadBlockRequest> makeRequestsWithMatchedHeader(IndexBlockV3Header header, List<Long> ids) {
		List<ReadBlockRequest> ret = new ArrayList<LogFileReaderV3o.ReadBlockRequest>();
		long blockMin = header.firstId;
		long blockMax = blockMin + header.logCount;

		List<List<Long>> idBunches = cutRange(ids, blockMin, blockMax);
		for (List<Long> idBunch : idBunches) {
			if (idBunch.isEmpty())
				continue;

			long bunchMaxID = idBunch.get(0);
			if (bunchMaxID < blockMax && bunchMaxID >= blockMin) {
				ret.add(new ReadBlockRequest(header, idBunch));
			} else {
				List<ReadBlockRequest> requests = makeRequests(idBunch);
				if (requests != null && !requests.isEmpty()) {
					ret.addAll(requests);
				}
			}

		}

		return ret;
	}

	private class LogParseResult {
		List<Log> result;
		LogParserBugException parseError;

		public LogParseResult() {
			this.result = new ArrayList<Log>();
			this.parseError = null;
		}

		public LogParseResult(int size) {
			this.result = new ArrayList<Log>(size);
			this.parseError = null;
		}

		public void addAll(LogParseResult r) {
			result.addAll(r.result);
			if (parseError == null && r.parseError != null)
				parseError = r.parseError;
		}
	}

	private class LogRecordBuffer {
		private final int FLUSH_COUNT = 500;
		private final int FLUSH_TIME = 100;

		private Date from;
		private Date to;
		private long lastFlushed;

		private List<LogRecord> buffer;

		private LogParseResult result;

		private LogParserBuilder builder;

		LogRecordBuffer(Date from, Date to, LogParserBuilder builder) {
			this.from = from;
			this.to = to;
			this.builder = builder;
			this.result = new LogParseResult();
			this.lastFlushed = System.currentTimeMillis();
		}

		void put(LogRecord record) {
			if ((System.currentTimeMillis() - lastFlushed) >= FLUSH_TIME || (buffer != null && buffer.size() >= FLUSH_COUNT))
				flush();

			if (buffer == null)
				buffer = new ArrayList<LogRecord>(FLUSH_COUNT);

			buffer.add(record);
		}

		void flush() {
			if (buffer == null)
				return;

			LogParseResult parseResult = null;
			List<LogRecord> records = buffer;
			buffer = null;
			LogItemParser parser = new LogItemParser(builder, tableName, from, to, records);
			try {
				parseResult = parser.callSafely();
			} catch(Exception e) {
				logger.warn("unexpected exception while fetching logs: " + this, e);
			}
				

			lastFlushed = System.currentTimeMillis();
			result.addAll(parseResult);
		}
	}

	private class LogFetcher {
		private DataBlockV3 block;
		private Date from;
		private Date to;

		private List<Long> ids;
		private LogParserBuilder builder;
		

		public LogFetcher(DataBlockV3 block, Date from, Date to, List<Long> ids, LogParserBuilder builder) {
			this.block = block;
			this.from = from;
			this.to = to;
			this.ids = ids;
			this.builder = builder;
		}

		protected LogParseResult callSafely() throws Exception {
			LogRecordBuffer buffer = new LogRecordBuffer(from, to, builder);

			synchronized (block) {
				// if block is compressed, uncompress block
				// 2016.02.12. v3o will not support encrypted block.
				block.uncompress(null);

				for (long id : ids) {
					ByteBuffer dataBuffer = block.getDataBuffer();
					dataBuffer.position(block.getLogOffset((int) (id - block.getMinId())));
					Date date = new Date(dataBuffer.getLong());
					byte[] b = new byte[dataBuffer.getInt()];
					dataBuffer.get(b);

					LogRecord record = new LogRecord(date, id, ByteBuffer.wrap(b));
					record.setDay(day);
					buffer.put(record);
				}
			}
			buffer.flush();

			return buffer.result;
		}
	}

	private class LogItemParser {
		LogParserBuilder builder;
		String tableName;
		Date from;
		Date to;
		List<LogRecord> records;

		public LogItemParser(LogParserBuilder builder, String tableName, Date from, Date to, List<LogRecord> records) {
			this.builder = builder;
			this.tableName = tableName;
			this.from = from;
			this.to = to;
			this.records = records;
		}

		protected LogParseResult callSafely() throws Exception {
			if (records == null)
				return null;

			LogParser parser = null;
			if (builder != null)
				parser = builder.build();

			LogParseResult parseResult = new LogParseResult(records.size());
			for (LogRecord record : records) {
				List<Log> result = null;
				try {
					result = parse(tableName, parser, LogMarshaler.convert(tableName, record));
				} catch (LogParserBugException e) {
					result = new ArrayList<Log>(1);
					result.add(new Log(e.tableName, e.date, e.id, e.logMap));

					if (parseResult.parseError == null)
						parseResult.parseError = e;
				} finally {
					if (result != null) {
						for (Log log : result) {
							if (from != null || to != null) {
								Date logDate = log.getDate();
								if (from != null && logDate.before(from))
									continue;
								if (to != null && !logDate.before(to))
									continue;
							}

							parseResult.result.add(log);
						}
					}
				}
			}

			return parseResult;
		}

	}

	private LogRecord getLogRecord(IndexBlockV3Header indexBlockHeader, long id) throws IOException {
		DataBlockV3 block = loadDataBlock(indexBlockHeader, dataStream);
		if (block == null)
			return null;

		// if block is compressed, uncompress block
		Date date = null;
		byte[] b = null;
		synchronized (block) {
			// 2016.02.12. v3o will not support encrypted block.
			block.uncompress(null);

			ByteBuffer dataBuffer  = block.getDataBuffer();
			dataBuffer.position(block.getLogOffset((int) (id - block.getMinId())));
			date = new Date(dataBuffer.getLong());
			b = new byte[dataBuffer.getInt()];
			dataBuffer.get(b);
		}
		return new LogRecord(date, id, ByteBuffer.wrap(b));
	}

	@Override
	public void close() {
		ensureClose(indexStream, indexPath);
		ensureClose(dataStream, dataPath);
	}

	private void ensureClose(Closeable stream, FilePath f) {
		try {
			if (stream != null)
				stream.close();
		} catch (IOException e) {
			logger.error("logpresso logstorage: cannot close file - " + f.getAbsolutePath(), e);
		}
	}

	/**
	 * descending order by default
	 * 
	 * @return log record cursor
	 * @throws IOException
	 */
	public LogRecordCursor getCursor() throws IOException {
		return getCursor(false);
	}

	public LogRecordCursor getCursor(boolean ascending) throws IOException {
		return new LogCursorImpl(ascending);
	}

	/**
	 * @since 2.6.0
	 */
	@Override
	public LogBlockCursor getBlockCursor() throws IOException {
		return new LogBlockCursorV3o(indexPath, dataPath);
	}

	private class LogCursorImpl implements LogRecordCursor {
		// offset of next log
		private long pos;

		private IndexBlockV3Header currentIndexHeader;
		private int currentIndexBlockNo;
		private final boolean ascending;

		private LogRecord cached;

		public LogCursorImpl(boolean ascending) throws IOException {
			this.ascending = ascending;

			if (indexBlockHeaders.size() == 0)
				return;

			if (ascending) {
				currentIndexHeader = indexBlockHeaders.get(0);
				currentIndexBlockNo = 0;
			} else {
				currentIndexHeader = indexBlockHeaders.get(indexBlockHeaders.size() - 1);
				currentIndexBlockNo = indexBlockHeaders.size() - 1;
			}

			cached = null;

			replaceBuffer();
		}

		public void skip(long offset) {
			if (offset == 0)
				return;

			if (offset < 0)
				throw new IllegalArgumentException("negative offset is not allowed");

			pos += offset;

			int relative = getRelativeOffset();
			if (relative >= currentIndexHeader.logCount)
				replaceBuffer();
		}

		@Override
		public void reset() {
			pos = 0;
			replaceBuffer();
		}

		private void replaceBuffer() {
			Integer next = findIndexBlock(pos);
			if (next == null)
				return;

			// read log data offsets from index block
			currentIndexBlockNo = next;
			currentIndexHeader = indexBlockHeaders.get(currentIndexBlockNo);
		}

		/**
		 * 
		 * @param offset
		 *            relative offset from file begin or file end
		 * @return the index block number
		 */
		private Integer findIndexBlock(long offset) {
			int no = currentIndexBlockNo;
			int blockCount = indexBlockHeaders.size();

			while (true) {
				if (no < 0 || no >= blockCount)
					return null;

				IndexBlockV3Header h = indexBlockHeaders.get(no);
				if (ascending) {
					if (offset < h.ascLogCount)
						no--;
					else if (h.logCount + h.ascLogCount <= offset)
						no++;
					else
						return no;
				} else {
					if (offset < h.dscLogCount)
						no++;
					else if (h.logCount + h.dscLogCount <= offset)
						no--;
					else
						return no;
				}
			}

		}

		@Override
		public boolean hasNext() {
			if (cached != null)
				return true;

			while (pos < totalCount) {
				int relative = getRelativeOffset();

				try {
					// absolute log offset in block (consider ordering)
					int n = ascending ? relative : (int) (currentIndexHeader.logCount - relative - 1);
					if (n < 0)
						throw new IllegalStateException("n " + n + ", current index no: " + currentIndexBlockNo
								+ ", current index count " + currentIndexHeader.logCount + ", relative " + relative);

					long id = ascending ? pos + 1 : totalCount - pos;
					cached = getLogRecord(currentIndexHeader, id);
					pos++;

					if (cached != null)
						return true;
				} catch (IOException e) {
					throw new IllegalStateException(e);
				} finally {
					// replace block if needed
					if (++relative >= currentIndexHeader.logCount) {
						replaceBuffer();
					}
				}
			}

			return false;
		}

		@Override
		public LogRecord next() {
			if (!hasNext())
				throw new IllegalStateException("log file is closed: " + dataPath.getAbsolutePath());
			LogRecord ret = cached;
			cached = null;
			return ret;
		}

		private int getRelativeOffset() {
			// accumulated log count except current block
			long accCount = ascending ? currentIndexHeader.ascLogCount : currentIndexHeader.dscLogCount;

			// relative offset in block
			int relative = (int) (pos - accCount);
			if (relative < 0)
				throw new IllegalStateException("relative bug check: " + relative + ", pos " + pos + ", acc: " + accCount);
			return relative;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("log remove() is not supported");
		}
	}

	@Override
	public List<Log> find(Date from, Date to, List<Long> ids, LogParserBuilder builder) {
		// ids should be in descending order
		long recentID = Long.MAX_VALUE;
		List<Long> filteredIDs = new ArrayList<Long>(ids.size());
		// check and filtering ids
		for (long id : ids) {
			if (id > recentID)
				throw new IllegalStateException(String.format("ids should be in descending order: %d->%d", recentID, id));

			recentID = id;
			if (id < 0)
				continue;

			filteredIDs.add(id);
		}

		List<Log> ret = new ArrayList<Log>(filteredIDs.size());
		List<ReadBlockRequest> requests = makeRequests(filteredIDs);
		if (requests == null || requests.isEmpty())
			return ret;

		for (ReadBlockRequest req : requests) {
			DataBlockV3 block = null;
			try {
				block = loadDataBlock(req.header, dataStream);
			} catch (IOException e) {
			}

			if (block == null)
				continue;

			LogFetcher fetcher = new LogFetcher(block, from, to, req.ids, builder);
			LogParseResult fetchResult = null;
			try {
				fetchResult = fetcher.callSafely();
			} catch (Exception e) {
				logger.warn("unexpected exception while fetching logs: " + this, e);
				continue;
			}
						
			handleParseError(builder, fetchResult);

			if (fetchResult.result != null && !fetchResult.result.isEmpty()) {
				ret.addAll(fetchResult.result);
			}
		}

		return ret;
	}

	@Override
	public void traverse(TableScanRequest req) throws IOException, InterruptedException {
		traverseNonParallel(req);
	}

	private void handleParseError(LogParserBuilder builder, LogParseResult parseResult) {
		if (parseResult.parseError != null && !builder.isBugAlertSuppressed()) {
			logger.error("araqne logstorage: PARSER BUG! original log => table " + parseResult.parseError.tableName + ", id "
					+ parseResult.parseError.id + ", data " + parseResult.parseError.logMap, parseResult.parseError.cause);
			builder.suppressBugAlert();
		}
	}

	private void traverseNonParallel(TableScanRequest req) throws IOException, InterruptedException {
		Date from = req.getFrom();
		Date to = req.getTo();
		long minId = req.getMinId();
		long maxId = req.getMaxId();
		LogParserBuilder builder = req.getParserBuilder();
		LogTraverseCallback callback = req.getTraverseCallback();
		
		boolean suppressBugAlert = false;
		LogParser parser = null;
		if (builder != null)
			parser = builder.build();

		for (int i = indexBlockHeaders.size() - 1; i >= 0; i--) {
			IndexBlockV3Header index = indexBlockHeaders.get(i);

			Long fromTime = (from == null) ? null : from.getTime();
			Long toTime = (to == null) ? null : to.getTime();
			if ((fromTime == null || index.maxTime >= fromTime) && (toTime == null || index.minTime < toTime)
					&& (maxId < 0 || index.firstId <= maxId) && (minId < 0 || index.firstId + index.logCount > minId)) {
				DataBlockV3 block = loadDataBlock(index, dataStream);
				if (block == null)
					continue;

				try {
					synchronized (block) {
						// if block is compressed, uncompress block
						// 2016.02.12. v3o will not support encrypted block. 
						block.uncompress(null);

						ByteBuffer currentDataBuffer = block.getDataBuffer();

						// reverse order
						ArrayList<Log> logs = new ArrayList<Log>();
						for (int j = block.getLogOffsetCount() - 1; j >= 0; j--) {
							currentDataBuffer.position(block.getLogOffset(j));
							long timestamp = currentDataBuffer.getLong();
							long id = block.getMinId() + j;
							int len = currentDataBuffer.getInt();

							if (from != null && timestamp < fromTime)
								continue;
							if (to != null && timestamp >= toTime)
								continue;
							if (minId >= 0 && id < minId) // descending order by
															// id
								break;
							if (maxId >= 0 && id > maxId)
								continue;

							// read record
							byte[] b = new byte[len];
							currentDataBuffer.get(b);

							LogRecord record = new LogRecord(new Date(timestamp), id, ByteBuffer.wrap(b));
							List<Log> result = null;
							try {
								result = parse(tableName, parser, LogMarshaler.convert(tableName, record));
							} catch (LogParserBugException e) {
								result = new ArrayList<Log>(1);
								result.add(new Log(e.tableName, e.date, e.id, e.logMap));
								if (!suppressBugAlert) {
									logger.error("araqne logstorage: PARSER BUG! original log => table " + e.tableName + ", id "
											+ e.id + ", data " + e.logMap, e.cause);
									suppressBugAlert = true;
								}
							} finally {
								if (result != null)
									logs.addAll(result);
							}
						}
						callback.writeLogs(logs);
						if (callback.isEof())
							return;
					}
				} finally {
				}
			}
		}
	}
}
