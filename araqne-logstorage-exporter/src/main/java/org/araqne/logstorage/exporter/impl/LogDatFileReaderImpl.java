package org.araqne.logstorage.exporter.impl;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.araqne.codec.EncodingRule;
import org.araqne.logstorage.Crypto;
import org.araqne.logstorage.exporter.CryptoParams;
import org.araqne.logstorage.exporter.LogBlock;
import org.araqne.logstorage.exporter.LogFileHeader;
import org.araqne.logstorage.exporter.api.LogDatFileReader;
import org.araqne.logstorage.file.Compression;
import org.araqne.logstorage.file.DeflaterCompression;
import org.araqne.logstorage.file.SnappyCompression;

public class LogDatFileReaderImpl implements LogDatFileReader {
	private final int FIXED_FILE_HEADER_LENGTH = 22;
	private final int OPTION_FLAG_LENGTH = 16;
	private final int DATA_BLOCK_HEADER_LENGTH = 24;
	private final int BLOCK_HEADER_LENGTH = 50;
	private final int ENCRYPT_FLAG = 0x80;
	private final int COMPRESS_FLAG = 0x20;
	private BufferedInputStream bis;
	private FileInputStream fis;
	private Compression compression;
	private int version;
	private CryptoParams crypto;

	public LogDatFileReaderImpl(File datFile) {
		this(datFile, null, null);
	}

	public LogDatFileReaderImpl(File datFile, File pfxFile, String password) {
		if (!datFile.exists())
			throw new IllegalStateException("not-found-file");

		if (datFile.length() < FIXED_FILE_HEADER_LENGTH) {
			System.out.println("invalid log data file: " + datFile.length());
			throw new IllegalStateException("invalid-log-file");
		}

		try {
			fis = new FileInputStream(datFile);
			bis = new BufferedInputStream(fis);

			LogFileHeader fileHeader = getFileHeader(bis);
			this.version = fileHeader.getVersion();
			validateLogFileHeader(fileHeader);
			version = fileHeader.getVersion();
			byte[] ext = fileHeader.getExt();

			if (version == 3) {
				byte[] keyByte = Arrays.copyOfRange(ext, 0, 4);
				int keyFlag = ByteBuffer.wrap(keyByte).getInt();
				if (keyFlag != 0 && (keyFlag & ENCRYPT_FLAG) == 0) {
					if (pfxFile == null || !pfxFile.exists())
						throw new IllegalStateException("not-found-pfxfile");

					if (pfxFile != null && password != null) {
						String keyPath = datFile.getName().substring(0, datFile.getName().lastIndexOf(".")) + ".key";
						File keyFile = new File(datFile.getParentFile(), keyPath);
						if (!keyFile.exists())
							throw new IllegalStateException("not-found-keyfile");
						crypto = LogKeyFileReader.getCryptoParams(keyFile, pfxFile, password);
					}
				}
			}

			String compressionMethod = new String(ext, 4, ext.length - 4).trim();
			if (compressionMethod.length() == 0)
				compressionMethod = null;

			compression = newCompression(compressionMethod);
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
		if (version == 2)
			return getV2Logs();
		else if (version == 3)
			return getV3Logs();
		else
			throw new UnsupportedOperationException("unsupported-log-version");
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

	private List<Map<String, Object>> getV3Logs() {
		try {
			LogBlock block = getV3LogBlock();
			ByteBuffer bb = uncompress(block);

			List<Map<String, Object>> logs = new ArrayList<Map<String, Object>>();
			for (int index = 0; index < block.getLogOffsets().length; index++) {
				bb.position(block.getLogOffsets()[index]);
				Map<String, Object> log = getV3Log(bb, block.getMinId() + index);
				logs.add(log);
			}
			return logs;
		} catch (IOException e) {
			return null;
		}
	}

	private List<Map<String, Object>> getV2Logs() {
		try {
			LogBlock block = getV2LogBlock();

			ByteBuffer bb = uncompress(block);
			List<Map<String, Object>> logs = new ArrayList<Map<String, Object>>();
			while (bb.position() != bb.limit()) {
				Map<String, Object> log = getV2Log(bb);
				logs.add(log);
			}
			return logs;
		} catch (IOException e) {
			return null;
		}
	}

	private Map<String, Object> getV3Log(ByteBuffer bb, long id) {
		long timeStamp = bb.getLong();
		int len = bb.getInt();
		byte[] b = new byte[len];
		bb.get(b);
		Map<String, Object> m = EncodingRule.decodeMap(ByteBuffer.wrap(b));
		m.put("_time", new Date(timeStamp));
		m.put("_id", id);
		return m;
	}

	private Map<String, Object> getV2Log(ByteBuffer bb) throws IOException {
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

		if (version != 2 && version != 3)
			throw new IllegalStateException("invalid-version");
	}

	private Compression newCompression(String compressionMethod) {
		if (compressionMethod == null)
			return null;

		if (compressionMethod.equals("snappy"))
			return new SnappyCompression();
		else
			return new DeflaterCompression();
	}

	private LogBlock getV2LogBlock() throws IOException {
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
		return block;
	}

	private LogBlock getV3LogBlock() throws IOException {
		LogBlock block = new LogBlock();
		byte[] logHeader = new byte[BLOCK_HEADER_LENGTH];
		bis.read(logHeader);
		ByteBuffer bb = ByteBuffer.wrap(logHeader);

		block.setBlockSize(bb.getInt());
		block.setVersion(bb.get());
		block.setOptionFlag(bb.get());
		block.setMinTime(bb.getLong());
		block.setMaxTime(bb.getLong());
		block.setMinId(bb.getLong());
		block.setMaxId(bb.getLong());
		block.setOriginalBlockSize(bb.getInt());
		block.setCompressedBlockSize(bb.getInt());
		block.setLogOffsetLength(bb.getInt());

		byte[] logOffsetBuffer = new byte[block.getLogOffsetLength()];
		bis.read(logOffsetBuffer);
		bb = ByteBuffer.wrap(logOffsetBuffer);

		int logCount = (int) (block.getMaxId() - block.getMinId() + 1);
		int[] logOffsets = new int[logCount];
		int bufpos = 0;
		int acc = 0;
		for (int i = 0; i < logCount; i++) {
			logOffsets[i] = acc;

			// read number (manual inlining)
			long n = 0;
			byte b;
			do {
				n <<= 7;
				b = logOffsetBuffer[bufpos++];
				n |= b & 0x7f;
			} while ((b & 0x80) != 0);

			acc += n;
		}
		block.setLogOffsets(logOffsets);

		// 암호화 확인!!
		if ((block.getOptionFlag() & 0x80) == 0x80) {
			byte[] iv = new byte[OPTION_FLAG_LENGTH];
			byte[] signature = new byte[OPTION_FLAG_LENGTH * 2];
			bis.read(iv);
			bis.read(signature);
			block.setIv(iv);
			block.setSignature(signature);
		}

		byte[] logData = new byte[block.getCompressedBlockSize()];
		bis.read(logData);
		block.setLogData(logData);
		return block;
	}

	private ByteBuffer uncompress(LogBlock block) throws IOException {
		if (crypto != null) {
			try {
				block.setLogData(Crypto._decrypt(block.getLogData(), crypto.getCipher(), crypto.getCipherKey(), block.getIv()));
			} catch (Throwable t) {
				throw new IOException("cannot decrypt block", t);
			}
		}

		byte[] logBlock = block.getLogData();
		if ((compression != null && (block.getOptionFlag() & COMPRESS_FLAG) == 0)) {
			logBlock = new byte[block.getOriginalBlockSize()];
			compression.uncompress(logBlock, block.getLogData(), 0, block.getLogData().length);
		}

		return ByteBuffer.wrap(logBlock);
	}
}
