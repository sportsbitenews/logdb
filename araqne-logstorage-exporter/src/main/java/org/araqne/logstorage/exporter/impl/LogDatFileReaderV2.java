package org.araqne.logstorage.exporter.impl;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.araqne.codec.EncodingRule;
import org.araqne.logstorage.exporter.LogBlock;
import org.araqne.logstorage.exporter.LogFileHeader;
import org.araqne.logstorage.exporter.api.LogDatFileReader;
import org.araqne.logstorage.file.Compression;
import org.araqne.logstorage.file.DeflaterCompression;

public class LogDatFileReaderV2 implements LogDatFileReader {
	private final int FIXED_FILE_HEADER_LENGTH = 22;
	private final int OPTION_FLAG_LENGTH = 16;
	private final int DATA_BLOCK_HEADER_LENGTH = 24;
	private int version = 2;
	private BufferedInputStream bis;
	private FileInputStream fis;
	private Compression compression;
	private boolean useDeflater;

	public LogDatFileReaderV2(File datFile) {
		if (!datFile.exists())
			throw new IllegalStateException("not-found-file");

		if (datFile.length() < FIXED_FILE_HEADER_LENGTH) {
			System.out.println("invalid log data file: " + datFile.length());
			throw new IllegalStateException("invalid-log-file");
		}

		try {
			fis = new FileInputStream(datFile);
			bis = new BufferedInputStream(fis);

			LogFileHeader header = getFileHeader(bis);
			validateLogFileHeader(header);
			byte[] ext = header.getExt();
			useDeflater = false;
			if (new String(ext, 4, ext.length - 4).trim().equals("deflater"))
				useDeflater = true;

			compression = new DeflaterCompression();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean hasNext() {
		try {
			if (bis.available() >= DATA_BLOCK_HEADER_LENGTH)
				return true;
		} catch (IOException e) {
			throw new IllegalStateException("cannot read next log block");
		}
		return false;
	}

	@Override
	public List<Map<String, Object>> nextBlock() {
		try {
			LogBlock block = getLogBlock();

			ByteBuffer bb = ByteBuffer.wrap(block.getLogData());
			List<Map<String, Object>> logs = new ArrayList<Map<String, Object>>();
			while (bb.position() != bb.limit()) {
				Map<String, Object> log = getLog(bb);
				logs.add(log);
			}
			return logs;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public void close() {
		if (fis != null)
			try {
				fis.close();
			} catch (IOException e) {
			}

		if (bis != null)
			try {
				bis.close();
			} catch (IOException e) {
			}
	}

	private LogBlock getLogBlock() throws IOException {
		byte[] blockHeader = new byte[DATA_BLOCK_HEADER_LENGTH];
		bis.read(blockHeader);
		ByteBuffer bb = ByteBuffer.wrap(blockHeader);
		LogBlock block = new LogBlock();
		// skip start and end date
		bb.getLong();
		bb.getLong();
		block.setOriginalBlockSize(bb.getInt());
		block.setCompressedBlockSize(bb.getInt());

		byte[] logByte = new byte[block.getCompressedBlockSize()];
		bis.read(logByte);
		block.setLogData(logByte);
		if (useDeflater || block.getCompressedBlockSize() != block.getOriginalBlockSize()) {
			byte[] orginLogByte = new byte[block.getOriginalBlockSize()];
			compression.uncompress(orginLogByte, logByte, 0, logByte.length);
			block.setLogData(orginLogByte);
		}
		return block;
	}

	private Map<String, Object> getLog(ByteBuffer bb) throws IOException {
		long id = bb.getLong();
		Date date = new Date(bb.getLong());
		byte[] b = new byte[bb.getInt()];
		bb.get(b);
		Map<String, Object> m = EncodingRule.decodeMap(ByteBuffer.wrap(b));
		m.put("_id", id);
		m.put("_time", date);
		return m;
	}

	private LogFileHeader getFileHeader(BufferedInputStream bis) throws IOException {
		byte[] fileHeader = new byte[FIXED_FILE_HEADER_LENGTH];
		bis.read(fileHeader);
		ByteBuffer bb = ByteBuffer.wrap(fileHeader);

		LogFileHeader header = new LogFileHeader();
		byte[] magicString = new byte[OPTION_FLAG_LENGTH];
		bb.get(magicString);

		header.setMagicString(new String(magicString));
		header.setBom(bb.getShort());
		header.setVersion(bb.getShort());
		header.setHeaderSize(bb.getShort());

		if (header.getHeaderSize() == bb.position() || header.getHeaderSize() == FIXED_FILE_HEADER_LENGTH) {
			System.out.println("empty log");
			return null;
		}

		byte[] ext = new byte[header.getHeaderSize() - FIXED_FILE_HEADER_LENGTH];
		bis.read(ext);
		header.setExt(ext);

		return header;
	}

	private void validateLogFileHeader(LogFileHeader header) {
		if (header == null)
			throw new IllegalStateException("empty-log");

		if (!header.getMagicString().equals("NCHOVY_BEAST_DAT"))
			throw new IllegalStateException("invalid-magic-string");

		if (header.getVersion() != version)
			throw new IllegalStateException("invalid-version");
	}
}
