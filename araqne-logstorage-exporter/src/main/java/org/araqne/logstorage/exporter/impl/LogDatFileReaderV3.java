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

public class LogDatFileReaderV3 implements LogDatFileReader {
	private final int FIXED_FILE_HEADER_LENGTH = 22;
	private final int BLOCK_HEADER_LENGTH = 50;
	private final int OPTION_FLAG_LENGTH = 16;
	private final int ENCRYPT_FLAG = 0x80;
	private final int COMPRESS_FLAG = 0x20;

	// TODO
	private int version = 3;
	private BufferedInputStream bis;
	private FileInputStream fis;
	private LogFileHeader fileHeader;
	private Compression compression;
	private LogBlock block;
	private ByteBuffer bb;
	private CryptoParams crypto;

	public LogDatFileReaderV3(File datFile) {
		this(datFile, null, null);
	}

	public LogDatFileReaderV3(File datFile, File pfxFile, String password) {
		if (!datFile.exists())
			throw new IllegalStateException("not-found-file");

		if (datFile.length() < FIXED_FILE_HEADER_LENGTH) {
			System.out.println("invalid log data file: " + datFile.length());
			throw new IllegalStateException("invalid-log-file");
		}

		fis = null;
		bis = null;

		try {
			fis = new FileInputStream(datFile);
			bis = new BufferedInputStream(fis);

			fileHeader = getFileHeader(bis);
			validateLogFileHeader(fileHeader);

			byte[] ext = fileHeader.getExt();
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
			String compressionMethod = new String(ext, 4, ext.length - 4).trim();
			if (compressionMethod.length() == 0)
				compressionMethod = null;

			compression = newCompression(compressionMethod);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public List<Map<String, Object>> nextBlock() {
		List<Map<String, Object>> logs = new ArrayList<Map<String, Object>>();
		for (int index = 0; index < block.getLogOffsets().length; index++) {
			bb.position(block.getLogOffsets()[index]);
			Map<String, Object> log = decode(bb, block.getMinId() + index);
			logs.add(log);
		}
		return logs;
	}

	@Override
	public boolean hasNext() {
		try {
			if (bis.available() > 0) {
				prepareNextBlock();
				return true;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
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

	private void prepareNextBlock() throws IOException {
		block = getLogBlock(bis);
		bb = uncompress(block, compression);
	}

	private Map<String, Object> decode(ByteBuffer bb, long id) {
		long timeStamp = bb.getLong();
		int len = bb.getInt();
		byte[] b = new byte[len];
		bb.get(b);
		Map<String, Object> m = EncodingRule.decodeMap(ByteBuffer.wrap(b));
		m.put("_time", new Date(timeStamp));
		m.put("id", id);
		return m;
	}

	private ByteBuffer uncompress(LogBlock block, Compression compression) throws IOException {
		if (crypto != null) {
			try {
				block.setLogData(Crypto.decrypt(block.getLogData(), crypto.getCipher(), crypto.getCipherKey(), block.getIv()));
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

	private Compression newCompression(String compressionMethod) {
		if (compressionMethod == null)
			return null;

		if (compressionMethod.equals("snappy"))
			return new SnappyCompression();
		else
			return new DeflaterCompression();
	}

	private LogBlock getLogBlock(BufferedInputStream bis) throws IOException {
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
