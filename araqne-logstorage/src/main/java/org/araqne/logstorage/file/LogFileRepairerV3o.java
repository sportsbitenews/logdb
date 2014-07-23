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

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import org.araqne.logstorage.LogFileFixReport;
import org.araqne.logstorage.file.LogFileHeader;
import org.araqne.logstorage.LogFileRepairer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * check log .idx and .dat file metadata and block size, and truncate broken
 * data blocks or generate index blocks.
 * 
 * @author everclear
 * 
 */
public class LogFileRepairerV3o implements LogFileRepairer {
	private final Logger logger = LoggerFactory.getLogger(LogFileRepairerV3o.class.getName());
	private static final int FILE_VERSION = 3;
	private static int INDEX_BLOCK_SIZE = 8 * 3 + 4; // Data Block Position + Min/Max Time + Log Count
	
	static final int BLOCK_LENGTH = 4;
	static final int BLOCK_VERSION = 1;
	static final int BLOCK_FLAG = 1;
	static final int MIN_TIME = 8;
	static final int MAX_TIME = 8;
	static final int MIN_ID = 8;
	static final int MAX_ID = 8;
	static final int ORIGINAL_SIZE = 4;
	static final int COMPRESS_SIZE = 4;
	static final int LENGTH_BLOCK_LENGTH = 4;
	static final int FIXED_DATA_BLOCK_HEADER_LENGTH = BLOCK_LENGTH + BLOCK_VERSION + BLOCK_FLAG + MIN_TIME + MAX_TIME + MIN_ID + MAX_ID
			+ ORIGINAL_SIZE + COMPRESS_SIZE + LENGTH_BLOCK_LENGTH;
	
	private boolean truncateIndexBlock(LogFileFixReport report, File indexPath, RandomAccessFile indexFile, LogFileHeader indexFileHeader) throws IOException {
		int indexOver = (int) ((indexFile.length() - indexFileHeader.size()) % INDEX_BLOCK_SIZE);
		if (indexOver != 0) {
			indexFile.setLength(indexFile.length() - indexOver);
			logger.trace("araqne logstorage: truncated immature last index block [{}], removed [{}] bytes", indexPath, indexOver);
			report.setTruncatedIndexBytes(indexOver);
			report.setFixed();
			return true;
		}
		
		return false;
	}
	
	private void generateDataBlocks(LogFileFixReport report, File indexPath, File dataPath,
			RandomAccessFile indexFile, RandomAccessFile dataFile, LogFileHeader indexFileHeader, LogFileHeader dataFileHeader)
			throws IOException {
		boolean broken = false;
		
		LogIndexBlockV3 lastIndexBlock = null;
		LogDataBlockHeaderV3 lastDataBlock = null;
		int lostLogCount = 0;
		int truncatedDataBytes = 0;
		// generate broken data blocks
		long indexFp = indexFile.length();
		List<BrokenDataBlockInfo> brokenBlocks = new ArrayList<BrokenDataBlockInfo>();
		
		while (indexFp > indexFileHeader.size()) {
			indexFp = indexFp - INDEX_BLOCK_SIZE;
			indexFile.seek(indexFp);
			lastIndexBlock = readIndexBlock(indexFile);
			lastDataBlock = null;
			long dataBlockPos = lastIndexBlock.getDataFp();
			// data block is missing
			if (dataBlockPos < dataFileHeader.size() || dataBlockPos >= dataFile.length()) {
				broken = true;
				brokenBlocks.add(new BrokenDataBlockInfo(indexFp, lastIndexBlock, null));
				continue;
			}

			long actualLastDataBlockSize = dataFile.length() - dataBlockPos;
			try {
				lastDataBlock = readDataBlockHeader(dataPath, dataFile, dataBlockPos);
			} catch (EOFException e) {
				broken = true;
				brokenBlocks.add(new BrokenDataBlockInfo(indexFp, lastIndexBlock, null));
				logger.trace("araqne logstorage: truncated immature last data block [{}], removed [{}] bytes", dataPath,
						actualLastDataBlockSize);
				continue;
			}

			// truncate invalid data block
			if (!lastDataBlock.isComplete() || dataFile.length() < (lastDataBlock.getPos() + lastDataBlock.getBlockLength())) {
				broken = true;
				brokenBlocks.add(new BrokenDataBlockInfo(indexFp, lastIndexBlock, lastDataBlock));
				logger.trace("araqne logstorage: expected last data block size [{}], actual last index block size [{}]",
						lastDataBlock.getBlockLength(), actualLastDataBlockSize);
				logger.trace("araqne logstorage: truncated immature last data block [{}], removed [{}] bytes", dataPath,
						actualLastDataBlockSize);
				continue;
			}

			break;
		}
		
		int addedDataBlocks = report.getAddedDataBlocks();
		long generatedDataBlockSize = report.getAddedDataBytes();
		// generate misconstructed data blocks
		if (!brokenBlocks.isEmpty()) {
			long lastMaxId = 0;
			// get last max id
			long lastIndexFp = brokenBlocks.get(brokenBlocks.size() - 1).indexFp - INDEX_BLOCK_SIZE;
			if (lastIndexFp >= indexFileHeader.size()) {
				indexFile.seek(lastIndexFp);
				lastIndexBlock = readIndexBlock(indexFile);
				lastDataBlock = readDataBlockHeader(dataPath, dataFile, lastIndexBlock.getDataFp());
				lastMaxId = lastDataBlock.getMaxId();
			}
			
			byte[] longbuf = new byte[8];
			byte[] intbuf = new byte[4];
			
			for (int i = brokenBlocks.size() - 1; i >= 0; i--) {
				BrokenDataBlockInfo currInfo = brokenBlocks.get(i);
				long minId = lastMaxId + 1;
				long maxId = lastMaxId + currInfo.indexBlock.count;
				int blockLength = FIXED_DATA_BLOCK_HEADER_LENGTH;
				
				if (currInfo.dataBlock == null) {
					if (i > 0) {
						blockLength = (int)(brokenBlocks.get(i-1).indexBlock.getDataFp() - currInfo.indexBlock.getDataFp());
					}
					
					currInfo.dataBlock = new LogDataBlockHeaderV3();
					currInfo.dataBlock.setBlockLength(blockLength);
					currInfo.dataBlock.setMinTime(currInfo.indexBlock.getMinTime());
					currInfo.dataBlock.setMaxTime(currInfo.indexBlock.getMaxTime());
					currInfo.dataBlock.setMinId(minId);
					currInfo.dataBlock.setMaxId(maxId);
					currInfo.dataBlock.setOriSize(0);
					currInfo.dataBlock.setCompressedLength(0);
					currInfo.dataBlock.setLengthBlockSize(0);
					currInfo.dataBlock.setPos(currInfo.indexBlock.getDataFp());

				}
				
				currInfo.dataBlock.setFlag((byte)(currInfo.dataBlock.getFlag() | 0x40));
				
				// rewrite data block
				long oldLength = dataFile.length();
				writeDataBlock(currInfo.indexBlock, currInfo.dataBlock, dataFile, longbuf, intbuf);
				
				lostLogCount += currInfo.indexBlock.getCount();
				addedDataBlocks++;
				generatedDataBlockSize += dataFile.length() - oldLength;
				lastMaxId = maxId;
			}
		}
		
		report.setLostLogCount(lostLogCount);
		report.setTruncatedDataBytes(truncatedDataBytes);
		report.setAddedDataBlocks(addedDataBlocks);
		report.setAddedDataBytes(generatedDataBlockSize);
		if (broken)
			report.setFixed();
	}

	private void generateIndexBlocks(LogFileFixReport report, boolean indexTruncated, 
			File indexPath, File dataPath, 
			RandomAccessFile indexFile,	RandomAccessFile dataFile, 
			LogIndexBlockV3 lastIndexBlock, LogDataBlockHeaderV3 lastDataBlock, long lastDataPos) throws IOException {
		byte[] longbuf = new byte[8];
		byte[] intbuf = new byte[4];
		int addedIndexBlocks = 0;
		
		long lastMaxId = 0;
		if (lastDataBlock != null)
			lastMaxId = lastDataBlock.getMaxId();
		int lastLogCnt = 5000;
		if (lastIndexBlock != null)
			lastLogCnt = lastIndexBlock.getCount(); 
		
		long lostLogCount = report.getLostLogCount();
		int addedDataBlocks = report.getAddedDataBlocks();
		long generatedDataBlockSize = report.getAddedDataBytes();
		while (indexTruncated || dataFile.length() > lastDataPos) {
			LogDataBlockHeaderV3 currBlock = null;
			try {
				currBlock = readDataBlockHeader(dataPath, dataFile, lastDataPos);
			} catch (EOFException e) {
				currBlock = new LogDataBlockHeaderV3();
				currBlock.setBlockLength(FIXED_DATA_BLOCK_HEADER_LENGTH);
				currBlock.setMinTime(0);
				currBlock.setMaxTime(0);
				currBlock.setMinId(lastMaxId + 1);
				currBlock.setMaxId(lastMaxId + lastLogCnt);
				currBlock.setOriSize(0);
				currBlock.setCompressedLength(0);
				currBlock.setLengthBlockSize(0);
				currBlock.setPos(lastDataPos);
				currBlock.setFlag((byte)0x40);
				
				long oldDataSize = dataFile.length();
				LogIndexBlockV3 indexBlock = new LogIndexBlockV3(0, lastDataPos, 0, 0, lastLogCnt, false);
				writeDataBlock(indexBlock, currBlock, dataFile, longbuf, intbuf);
				
				logger.trace("logpresso logstorage: write dummy data block for {}, log count [{}], data file [{}]", new Object[] {
						currBlock, lastLogCnt, dataPath });

				addedDataBlocks++;
				generatedDataBlockSize += dataFile.length() - oldDataSize;
				lostLogCount += lastLogCnt;
			}
			
			report.setTotalLogCount(currBlock.getMaxId());
			lastDataPos = currBlock.getPos() + currBlock.getBlockLength();
			if (lastDataPos > dataFile.length()) {
				int logCnt = (int)(currBlock.getMaxId() - currBlock.getMinId() + 1);
				currBlock.setFlag((byte)(currBlock.getFlag() | 0x40));

				long oldDataSize = dataFile.length();
				LogIndexBlockV3 indexBlock = new LogIndexBlockV3(0, currBlock.getPos(), 0, 0, logCnt, false);
				writeDataBlock(indexBlock, currBlock, dataFile, longbuf, intbuf);
				logger.trace("logpresso logstorage: write dummy data block for {}, log count [{}], data file [{}]", new Object[] {
						currBlock, logCnt, dataPath });

				addedDataBlocks++;
				generatedDataBlockSize += dataFile.length() - oldDataSize;
				lostLogCount += logCnt;
			}

			int logCount = writeIndexBlock(currBlock, indexFile, longbuf, intbuf);
			addedIndexBlocks++;
			
			logger.trace("logpresso logstorage: rewrite index block for {}, log count [{}], index file [{}]", new Object[] {
					currBlock, logCount, indexPath });

			lastMaxId = currBlock.getMaxId();
			lastLogCnt = logCount;
			indexTruncated = false;
		}

		report.setAddedIndexBlocks(addedIndexBlocks);
		report.setLostLogCount(lostLogCount);
		report.setAddedDataBlocks(addedDataBlocks);
		report.setAddedDataBytes(generatedDataBlockSize);
		if (addedIndexBlocks > 0)
			report.setFixed();
	}
	
	@Override
	public LogFileFixReport quickFix(File indexPath, File dataPath) throws IOException {
		RandomAccessFile indexFile = null;
		RandomAccessFile dataFile = null;
		LogFileFixReport report = new LogFileFixReport();

		try {
			indexFile = new RandomAccessFile(indexPath, "rw");
			dataFile = new RandomAccessFile(dataPath, "rw");

			LogFileHeader indexFileHeader = LogFileHeader.extractHeader(indexFile, indexPath);
			LogFileHeader dataFileHeader = LogFileHeader.extractHeader(dataFile, dataPath);
			if (indexFileHeader.version() != FILE_VERSION || dataFileHeader.version() != FILE_VERSION)
				return null;

			report.setIndexPath(indexPath);
			report.setDataPath(dataPath);
			
			boolean indexTruncated = truncateIndexBlock(report, indexPath, indexFile, indexFileHeader);
			generateDataBlocks(report, indexPath, dataPath, indexFile, dataFile, indexFileHeader, dataFileHeader);

			// get last data block
			LogIndexBlockV3 lastIndexBlock = null;
			LogDataBlockHeaderV3 lastDataBlock = null;
			if (indexFile.length() >= indexFileHeader.size() + INDEX_BLOCK_SIZE) {
				indexFile.seek(indexFile.length() - INDEX_BLOCK_SIZE);
				lastIndexBlock = readIndexBlock(indexFile);
				if (lastIndexBlock.dataFp < dataFile.length())
					lastDataBlock = readDataBlockHeader(dataPath, dataFile, lastIndexBlock.dataFp);
			}
			
			// generate missing index blocks
			// TODO: make index header when index file is empty
			indexFile.seek(indexFile.length());			
			long lastDataPos = dataFileHeader.size();
			if (lastDataBlock != null) {
				lastDataPos = lastDataBlock.getPos() + lastDataBlock.getBlockLength();
				report.setTotalLogCount(lastDataBlock.maxId);
			}
			
			generateIndexBlocks(report, indexTruncated, indexPath, dataPath, indexFile, dataFile, lastIndexBlock, lastDataBlock, lastDataPos);
			return report;
		} catch (UnexpectedFormatException e) { 
			logger.trace("logpresso logstorage: unexpected block format [{} : {}], aborting", e.getFile(),
					e.getPos());
			return null;
		} finally {
			if (indexFile != null) {
				if (report.isFixed())
					indexFile.getFD().sync();
				
				indexFile.close();
			}
			if (dataFile != null) {
				if (report.isFixed())
					dataFile.getFD().sync();
				
				dataFile.close();
			}
		}		
	}
	
	@Override
	public LogFileFixReport fix(File indexPath, File dataPath) throws IOException {
		return quickFix(indexPath, dataPath);
		// TODO: implement fullscan code
		
		/*
		RandomAccessFile indexFile = null;
		RandomAccessFile dataFile = null;

		try {
			indexFile = new RandomAccessFile(indexPath, "rw");
			dataFile = new RandomAccessFile(dataPath, "rw");

			LogFileHeader indexFileHeader = LogFileHeader.extractHeader(indexFile, indexPath);
			LogFileHeader dataFileHeader = LogFileHeader.extractHeader(dataFile, dataPath);
			if (indexFileHeader.version() != FILE_VERSION || dataFileHeader.version() != FILE_VERSION)
				return null;

			// TODO : when index file is missing
			indexFile.seek(indexFileHeader.size());

			List<LogIndexBlockV3> indexBlocks = readIndexBlocks(indexFile);
			List<LogDataBlockHeaderV3> dataBlockHeaders = readDataBlockHeaders(dataPath, dataFile, dataFileHeader);

			// check broken data file
			long truncatedData = truncateMisConstructedDataBlock(dataPath, dataFile, dataBlockHeaders);
			if (indexBlocks.size() == dataBlockHeaders.size()) {
				return null;
			}
			
			LogFileFixReport report = null;
			if (indexBlocks.size() < dataBlockHeaders.size())
				report = generate(indexPath, dataPath, indexFile, dataFile, indexBlocks, dataBlockHeaders);
			else
				report = truncate(indexPath, dataPath, indexFile, dataFile, indexBlocks, dataBlockHeaders);
			
			report.setTruncatedDataBytes(report.getTruncatedDataBytes() + (int)truncatedData);
			return report;
		} finally {
			if (indexFile != null)
				indexFile.close();
			if (dataFile != null)
				dataFile.close();
		}
		*/
	}
	
	private long truncateMisConstructedDataBlock(File dataPath, RandomAccessFile dataFile, List<LogDataBlockHeaderV3> dataBlockHeaders) throws IOException {
		if (dataBlockHeaders.isEmpty())
			return 0;
		
		LogDataBlockHeaderV3 lastDataBlockHeader = dataBlockHeaders.get(dataBlockHeaders.size() - 1);
		long logicalEndOfData = lastDataBlockHeader.getPos() + lastDataBlockHeader.getBlockLength();
		long dataOver = dataFile.length() - logicalEndOfData;
		if (dataOver > 0) {
			dataFile.setLength(logicalEndOfData);
			logger.info("logpresso logstorage: truncated immature last data block [{}], removed [{}] bytes", dataPath, dataOver);
		}
		
		return dataOver; 
	}

	private LogFileFixReport truncate(File indexPath, File dataPath, RandomAccessFile indexFile, RandomAccessFile dataFile,
			List<LogIndexBlockV3> indexBlocks, List<LogDataBlockHeaderV3> dataBlockHeaders) throws IOException {
		long validLogCount = 0;
		long totalLogCount = 0;
		
		LogFileFixReport report = new LogFileFixReport();
		
		// count only matched index blocks
		for (int i = 0; i < indexBlocks.size(); i++) {
			LogIndexBlockV3 b = indexBlocks.get(i);
			long count = b.getCount();
			totalLogCount += count;
			if (i < dataBlockHeaders.size())
				validLogCount += count;
		}
		
		long logicalEndOfIndex = indexBlocks.get(dataBlockHeaders.size()).getIndexFp();
		long indexOver = indexFile.length() - logicalEndOfIndex;
		
		if (indexOver > 0) {
			indexFile.setLength(logicalEndOfIndex);
		}
		
		// truncate data file
		LogDataBlockHeaderV3 lastDataBlockHeader = dataBlockHeaders.get(dataBlockHeaders.size() - 1);
		long logicalEndOfData = lastDataBlockHeader.getPos() + lastDataBlockHeader.getBlockLength();
		
		long dataOver = dataFile.length() - logicalEndOfData;
		if (dataOver > 0) {
			dataFile.setLength(logicalEndOfData);
		}
		
		report.setIndexPath(indexPath);
		report.setDataPath(dataPath);
		report.setTotalLogCount(totalLogCount);
		report.setTotalIndexBlocks(indexBlocks.size());
		report.setTotalDataBlocks(dataBlockHeaders.size());
		report.setLostLogCount((int) (totalLogCount - validLogCount));
		report.setTruncatedIndexBlocks(indexBlocks.size() - dataBlockHeaders.size());
		report.setTruncatedIndexBytes((int)indexOver);
		report.setTruncatedDataBytes((int)dataOver);
		
		return report;
	}


	private int writeIndexBlock(LogDataBlockHeaderV3 h, RandomAccessFile indexFile, byte[] longbuf, byte[] intbuf) throws IOException {
		prepareLong(h.getPos(), longbuf);
		indexFile.write(longbuf);
		
		prepareLong(h.getMinTime(), longbuf);
		indexFile.write(longbuf);
		prepareLong(h.getMaxTime(), longbuf);
		indexFile.write(longbuf);
		
		// assume there is no missing log in data block
		int logCount = (int)(h.getMaxId() - h.getMinId() + 1); 
		prepareInt(logCount, intbuf);
		indexFile.write(intbuf);
		
		return logCount;
	}
	
	private void writeDataBlock(LogIndexBlockV3 indexBlock, LogDataBlockHeaderV3 dataBlock, RandomAccessFile dataFile, byte[] longbuf, byte[] intbuf) throws IOException {
		dataFile.seek(indexBlock.getDataFp());
		
		prepareInt(dataBlock.getBlockLength(), intbuf);
		dataFile.write(intbuf);
		dataFile.write(3);
		dataFile.write(dataBlock.getFlag());

		// write min datetime
		prepareLong(dataBlock.getMinTime(), longbuf);
		dataFile.write(longbuf);

		// write max datetime
		prepareLong(dataBlock.getMaxTime(), longbuf);
		dataFile.write(longbuf);

		// write min id
		prepareLong(dataBlock.getMinId(), longbuf);
		dataFile.write(longbuf);

		// write max id
		prepareLong(dataBlock.getMaxId(), longbuf);
		dataFile.write(longbuf);

		// write original size
		prepareInt(dataBlock.getOriSize(), intbuf);
		dataFile.write(intbuf);

		// write compressed size
		prepareInt(dataBlock.getCompressedLength(), intbuf);
		dataFile.write(intbuf);

		// write length block length
		prepareInt(dataBlock.getLengthBlockSize(), intbuf);
		dataFile.write(intbuf);
		
		// fill block by dummy value
		if (dataBlock.getBlockLength() > FIXED_DATA_BLOCK_HEADER_LENGTH) {
			dataFile.seek(indexBlock.getDataFp() + dataBlock.getBlockLength() - 1);
			dataFile.write(0);
		}
	}
	
	private LogFileFixReport generate(File indexPath, File dataPath, RandomAccessFile indexFile, RandomAccessFile dataFile,
			List<LogIndexBlockV3> indexBlocks, List<LogDataBlockHeaderV3> dataBlockHeaders) throws IOException {
		logger.trace("logpresso logstorage: checking incomplete index block, file [{}]", indexPath);
		
		// truncate data file
		LogDataBlockHeaderV3 lastDataBlockHeader = dataBlockHeaders.get(dataBlockHeaders.size() - 1);
		long logicalEndOfData = lastDataBlockHeader.getPos() + lastDataBlockHeader.getBlockLength();
		
		long dataOver = dataFile.length() - logicalEndOfData;
		if (dataOver > 0) {
			dataFile.setLength(logicalEndOfData);
		}
		
		// check immature last index block writing
		LogIndexBlockV3 lastIndexBlock = indexBlocks.get(indexBlocks.size() - 1);
		long lastIndexBlockSize = indexFile.length() - lastIndexBlock.getIndexFp();
		
		// truncate immature last index block
		int truncatedIndexBytes = 0;
		if (lastIndexBlockSize != INDEX_BLOCK_SIZE) {
			logger.trace("logpresso logstorage: expected last index block size [{}], actual last index block size [{}]",
					INDEX_BLOCK_SIZE, lastIndexBlockSize);
			
			truncatedIndexBytes = (int)lastIndexBlockSize;
			indexFile.setLength(lastIndexBlock.getIndexFp());
			indexBlocks.remove(indexBlocks.size() - 1);
		
			logger.info("logpresso logstorage: truncated immature last index block [{}], removed [{}] bytes", indexPath,
					lastIndexBlockSize);
		}
		
		int missingBlockCount = dataBlockHeaders.size() - indexBlocks.size();
		logger.info("logpresso logstorage: index block [{}], data block [{}], missing count [{}]",
				new Object[] { indexBlocks.size(), dataBlockHeaders.size(), missingBlockCount });

		
		byte[] longbuf = new byte[8];
		byte[] intbuf = new byte[4];
		
		long pos = indexBlocks.get(indexBlocks.size() - 1).getIndexFp() + INDEX_BLOCK_SIZE;
		indexFile.seek(pos);
		long addedLogs = 0;
		for (int i = indexBlocks.size(); i < dataBlockHeaders.size(); i++) {
			LogDataBlockHeaderV3 h = dataBlockHeaders.get(i);
			int logCount = writeIndexBlock(h, indexFile, longbuf, intbuf);
			addedLogs += logCount;
			logger.info("logpresso logstorage: rewrite index block for {}, log count [{}], index file [{}]", new Object[] {
					h, logCount, indexPath });
		}
		
		long indexedLogs = 0;
		for (LogIndexBlockV3 b : indexBlocks) {
			indexedLogs += b.getCount();
		}
		
		LogFileFixReport report = new LogFileFixReport();
		report.setIndexPath(indexPath);
		report.setDataPath(dataPath);
		report.setTotalLogCount(indexedLogs + addedLogs);
		report.setTotalIndexBlocks(indexBlocks.size() + missingBlockCount);
		report.setTotalDataBlocks(dataBlockHeaders.size());
		report.setAddedIndexBlocks(missingBlockCount);
		report.setTruncatedIndexBytes(truncatedIndexBytes);
		
		return report;
	}

	private List<LogDataBlockHeaderV3> readDataBlockHeaders(File dataPath, RandomAccessFile dataFile, LogFileHeader dataFileHeader) throws IOException {
		long pos = dataFileHeader.size();
		long fileLength = dataFile.length();
		List<LogDataBlockHeaderV3> headers = new ArrayList<LogDataBlockHeaderV3>();
		int index = 0;
		try {
			for (;;) {
				LogDataBlockHeaderV3 header = readDataBlockHeader(dataPath, dataFile, pos);
				if (header == null)
					break;
				header.setIndex(index++);
				pos += header.getBlockLength();
				if (pos > fileLength || pos < dataFileHeader.size())
					break;

				headers.add(header);
				dataFile.seek(pos);
			}
		} catch (EOFException e) {

		}

		return headers;
	}

	private class UnexpectedFormatException extends IOException {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private final long pos;
		private final File file;
		
		public UnexpectedFormatException(File file, long pos) {
			this.file = file;
			this.pos = pos;
		}

		public File getFile() {
			return file;
		}
		
		public long getPos() {
			return pos;
		}

	}
	
	private LogDataBlockHeaderV3 readDataBlockHeader(File dataPath, RandomAccessFile dataFile, long pos) throws IOException {
		dataFile.seek(pos);
		int blockLength = dataFile.readInt();
		if (blockLength < 0)
			throw new UnexpectedFormatException(dataPath, pos);
		byte version = dataFile.readByte();
		if (version != FILE_VERSION)
			throw new UnexpectedFormatException(dataPath, pos);
		
		boolean complete = true;
		byte flag = dataFile.readByte();
		long minTime = dataFile.readLong();
		long maxTime = dataFile.readLong();
		long minId = dataFile.readLong();
		long maxId = dataFile.readLong();
		int oriSize = 0;
		int compressedLength = 0;
		int lengthBlockSize = 0;
		try {
			oriSize = dataFile.readInt();
			compressedLength = dataFile.readInt();
			lengthBlockSize = dataFile.readInt();
		} catch (EOFException e) {
			complete = false;
		}
		
		LogDataBlockHeaderV3 header = new LogDataBlockHeaderV3();
		header.setBlockLength(blockLength);
		header.setFlag(flag);
		header.setMinTime(minTime);
		header.setMaxTime(maxTime);
		header.setMinId(minId);
		header.setMaxId(maxId);
		header.setOriSize(oriSize);
		header.setCompressedLength(compressedLength);
		header.setLengthBlockSize(lengthBlockSize);
		header.setPos(pos);
		if (complete)
			header.setComplete();
		
		return header;
	}

	private List<LogIndexBlockV3> readIndexBlocks(RandomAccessFile indexFile) throws IOException {
		List<LogIndexBlockV3> indexBlocks = new ArrayList<LogIndexBlockV3>();
		int index = 0;
		try {
			for (;;) {
				LogIndexBlockV3 block = readIndexBlock(indexFile);
				block.setIndex(index++);
				indexBlocks.add(block);
			}
		} catch (EOFException e) {
		}

		return indexBlocks;
	}	
	
	private LogIndexBlockV3 readIndexBlock(RandomAccessFile indexFile) throws IOException {
		long indexFp = indexFile.getFilePointer();
		long dataFp = indexFile.readLong();
		long minTime = indexFile.readLong();
		long maxTime = indexFile.readLong();
		int count = indexFile.readInt();
		return new LogIndexBlockV3(indexFp, Math.abs(dataFp), minTime, maxTime, count, dataFp < 0);
	}

	private static void prepareInt(int l, byte[] b) {
		for (int i = 0; i < 4; i++)
			b[i] = (byte) ((l >> ((3 - i) * 8)) & 0xff);
	}

	private static void prepareLong(long l, byte[] b) {
		for (int i = 0; i < 8; i++)
			b[i] = (byte) ((l >> ((7 - i) * 8)) & 0xff);
	}

	private static class BrokenDataBlockInfo {
		long indexFp;
		LogIndexBlockV3 indexBlock;
		LogDataBlockHeaderV3 dataBlock;
		
		public BrokenDataBlockInfo(long indexFp, LogIndexBlockV3 indexBlock, LogDataBlockHeaderV3 dataBlock) {
			this.indexFp = indexFp;
			this.indexBlock = indexBlock;
			this.dataBlock = dataBlock;
		}
	}

	private class LogDataBlockHeaderV3 {
		int index;
		long pos;
		int blockLength;
		byte flag;
		long minTime;
		long maxTime;
		long minId;
		long maxId;
		int oriSize;
		int compressedLength;
		int lengthBlockSize;
		boolean complete = false;

		public long getMinTime() {
			return minTime;
		}

		public void setMinTime(long minTime) {
			this.minTime = minTime;
		}

		public long getMaxTime() {
			return maxTime;
		}

		public void setMaxTime(long maxTime) {
			this.maxTime = maxTime;
		}

		public long getMinId() {
			return minId;
		}

		public void setMinId(long minId) {
			this.minId = minId;
		}

		public long getMaxId() {
			return maxId;
		}

		public void setMaxId(long maxId) {
			this.maxId = maxId;
		}

		public int getOriSize() {
			return oriSize;
		}

		public void setOriSize(int oriSize) {
			this.oriSize = oriSize;
		}

		public byte getFlag() {
			return flag;
		}

		public void setFlag(byte flag) {
			this.flag = flag;
		}

		public int getLengthBlockSize() {
			return lengthBlockSize;
		}

		public void setLengthBlockSize(int lengthBlockSize) {
			this.lengthBlockSize = lengthBlockSize;
		}

		public void setBlockLength(int blockLength) {
			this.blockLength = blockLength;
		}

		public void setCompressedLength(int compressedLength) {
			this.compressedLength = compressedLength;
		}

		public int getCompressedLength() {
			return compressedLength;
		}
		
		public int getBlockLength() {
			return blockLength;
		}

		public int getIndex() {
			return index;
		}

		public void setIndex(int index) {
			this.index = index;
		}
		
		public long getPos() {
			return pos;
		}

		public void setPos(long pos) {
			this.pos = pos;
		}
		
		public boolean isComplete() {
			return complete;
		}
		
		public void setComplete() {
			this.complete = true;
		}
	}
	
	// TODO : use IndexBlockV3Header
	private class LogIndexBlockV3 {
		private long indexFp;
		private long dataFp;
		private long minTime;
		private long maxTime;
		private int count;
		private int index;
		private boolean isReserved;
		
		public LogIndexBlockV3(long indexFp, long dataFp, long minTime, long maxTime, int count, boolean isReserved) {
			this.indexFp = indexFp;
			this.dataFp = dataFp;
			this.minTime = minTime;
			this.maxTime = maxTime;
			this.count = count;
			this.isReserved = isReserved;
		}

		public long getIndexFp() {
			return indexFp;
		}

		public long getDataFp() {
			return dataFp;
		}

		public long getMinTime() {
			return minTime;
		}

		public long getMaxTime() {
			return maxTime;
		}

		public int getCount() {
			return count;
		}

		public int getIndex() {
			return index;
		}
		
		public boolean isReserved() {
			return isReserved;
		}

		public void setIndex(int index) {
			this.index = index;
		}
	}
}
