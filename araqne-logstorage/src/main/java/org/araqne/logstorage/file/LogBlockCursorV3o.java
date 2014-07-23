package org.araqne.logstorage.file;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.araqne.codec.Base64;
import org.araqne.logstorage.Crypto;
import org.araqne.logstorage.LogCryptoProfile;
import org.araqne.logstorage.file.DataBlockV3;
import org.araqne.logstorage.file.DataBlockV3Params;
import org.araqne.logstorage.file.IndexBlockV3Header;
import org.araqne.logstorage.file.InvalidLogFileHeaderException;
import org.araqne.logstorage.file.LogBlock;
import org.araqne.logstorage.file.LogBlockCursor;
import org.araqne.logstorage.file.LogFileHeader;
import org.araqne.storage.api.FilePath;
import org.araqne.storage.api.StorageInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogBlockCursorV3o implements LogBlockCursor {
	private final Logger logger = LoggerFactory.getLogger(LogBlockCursorV3o.class);
	private static final int FILE_VERSION = 3;
	public static final int INDEX_ITEM_SIZE = 28;

	private FilePath indexPath;
	private FilePath dataPath;
	private String compressionMethod;

	private List<IndexBlockV3Header> indexBlockHeaders = new ArrayList<IndexBlockV3Header>();

	private long totalCount;
	private StorageInputStream dataStream;

	private int currentBlockIndex;

	public LogBlockCursorV3o(FilePath indexPath, FilePath dataPath) throws IOException {
		this.indexPath = indexPath;
		this.dataPath = dataPath;

		loadIndexFile();
		loadDataFile();
	}

	private void loadIndexFile() throws IOException {
		BufferedInputStream indexReader = null;
		StorageInputStream indexStream = null;
		
		try {
			indexStream = indexPath.newInputStream();

			LogFileHeader indexFileHeader = LogFileHeader.extractHeader(indexStream);
			if (indexFileHeader.version() != FILE_VERSION)
				throw new InvalidLogFileHeaderException("version not match, index file " + indexPath.getAbsolutePath());

			long length = indexStream.length();
			long pos = indexFileHeader.size();

			indexReader = new BufferedInputStream(indexPath.newInputStream());
			indexReader.skip(pos);

			ByteBuffer bb = ByteBuffer.allocate(INDEX_ITEM_SIZE);
			int id = 0;
			while (pos < length) {
				indexReader.read(bb.array());
				IndexBlockV3Header header = new IndexBlockV3Header(id++, bb.getLong(), bb.getLong(), bb.getLong(), bb.getInt(),
						totalCount + 1);
				header.fp = pos;
				header.ascLogCount = totalCount;
				
				// skip reserved blocks
				if (!header.isReserved()) {
					totalCount += header.logCount;
					indexBlockHeaders.add(header);
				}
				
				pos += INDEX_ITEM_SIZE;
				bb.clear();
			}

			long t = 0;
			for (int i = indexBlockHeaders.size() - 1; i >= 0; i--) {
				IndexBlockV3Header h = indexBlockHeaders.get(i);
				h.dscLogCount = t;
				t += h.logCount;
			}
		} finally {
			if (indexReader != null) {
				try {
					indexReader.close();
				} catch (IOException e) {
				}
			}

			if (indexStream != null) {
				try {
					indexStream.close();
				} catch (IOException e) {
				}
			}
		}
	}

	private void loadDataFile() throws IOException {
		this.dataStream = dataPath.newInputStream();
		LogFileHeader dataFileHeader = LogFileHeader.extractHeader(dataStream);
		if (dataFileHeader.version() != FILE_VERSION)
			throw new InvalidLogFileHeaderException("version not match, data file");

		byte[] ext = dataFileHeader.getExtraData();

		compressionMethod = new String(ext, 4, ext.length - 4).trim();
		if (compressionMethod.length() == 0)
			compressionMethod = null;
	}

	private DataBlockV3 loadDataBlock(IndexBlockV3Header h) throws IOException {
		DataBlockV3Params p = new DataBlockV3Params();
		p.indexHeader = h;
		p.dataStream = dataStream;
		p.dataPath = dataPath;
		p.compressionMethod = compressionMethod;
		DataBlockV3 b = new DataBlockV3(p);
		return b;
	}

	@Override
	public void close() throws IOException {
		dataStream.close();
	}

	@Override
	public boolean hasNext() {
		return currentBlockIndex < indexBlockHeaders.size();
	}

	@Override
	public LogBlock next() {
		if (!hasNext())
			throw new NoSuchElementException();

		LogBlock lb = new LogBlock();

		IndexBlockV3Header indexHeader = indexBlockHeaders.get(currentBlockIndex);
		Map<String, Object> m = lb.getData();
		m.put("block_id", currentBlockIndex);
		m.put("min_time", new Date(indexHeader.minTime));
		m.put("max_time", new Date(indexHeader.maxTime));
		m.put("log_count", indexHeader.logCount);

		currentBlockIndex++;

		try {
			DataBlockV3 db = loadDataBlock(indexHeader);
			m.put("ver", (int) db.getVersion());
			m.put("fp", db.getDataFp());
			m.put("flag", Integer.toHexString(db.getFlag()));
			m.put("iv", db.getIv());
			m.put("signature", db.getSignature());
			m.put("original_size", db.getOriginalSize());
			m.put("compressed_size", db.getCompressedSize());
			m.put("data", db.getCompressedBuffer()); // FIXME : non-compression case (check LogFileReaderV3)
		} catch (Throwable t) {
			logger.debug("logpresso logstorage: broken data block, file=" + dataPath.getAbsolutePath() + ", fp=" + indexHeader.dataFp, t);
		}

		return lb;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}
