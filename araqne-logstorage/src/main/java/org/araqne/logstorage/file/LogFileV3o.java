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

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Date;

import org.araqne.storage.api.FilePath;
import org.araqne.storage.api.StorageInputStream;
import org.araqne.storage.api.StorageUtil;
import org.araqne.storage.filepair.CloseableEnumeration;
import org.araqne.storage.filepair.FilePair;

// TODO : handling encryption
public class LogFileV3o extends FilePair<IndexBlockV3Header, LogFileV3o.RawDataBlock> implements Closeable {
	private static final short FILE_VERSION = 3;

	private StorageInputStream dataStream;
	private LogFileHeader ifileHeader;
	private LogFileHeader dfileHeader;

	private String compression;
	private int compressionLevel;

	public LogFileV3o(FilePath indexFile, FilePath dataFile) throws IOException {
		this(indexFile, dataFile, "deflate");
	}

	public LogFileV3o(int id, Date day, FilePath basePath) throws IOException {
		this(id, day, basePath, "deflate");
	}

	public LogFileV3o(int id, Date day, FilePath basePath, String compression) throws IOException {
		this(DatapathUtil.getIndexFile(id, day, basePath), DatapathUtil.getDataFile(id, day, basePath), compression);
	}

	public LogFileV3o(FilePath indexFile, FilePath dataFile, String compression) throws IOException {
		super(indexFile, dataFile, IndexBlockV3Header.class, RawDataBlock.class);
		this.compression = compression;
		this.compressionLevel = 3;
	}

	private StorageInputStream getDataStream() throws IOException {
		if (dataStream == null) {
			try {
				if (dfile.exists())
					dataStream = dfile.newInputStream();
			} catch (Throwable t) {
				StorageUtil.ensureClose(dataStream);

				throw new IOException();
			}
		}

		return dataStream;
	}

	LogFileHeader getIndexFileHeader() throws IOException {
		if (ifileHeader == null)
			ifileHeader = LogFileHeader.extractHeader(ifile);
		return ifileHeader;
	}

	// for test
	void setIndexFileHeader(LogFileHeader header) {
		if (ifileHeader != null)
			throw new IllegalStateException("IndexFileHeader is already set");
		ifileHeader = header;
	}

	LogFileHeader getDataFileHeader() throws IOException {
		if (dfileHeader == null)
			dfileHeader = LogFileHeader.extractHeader(dfile);
		return dfileHeader;
	}

	// for test
	void setDataFileHeader(LogFileHeader header) {
		if (dfileHeader != null)
			throw new IllegalStateException("DataFileHeader is already set");
		dfileHeader = header;
	}

	@Override
	public void close() throws IOException {
		StorageUtil.ensureClose(dataStream);
		dataStream = null;
	}

	@Override
	public void writeIndexFileHeader(OutputStream os) throws IOException {
		LogFileHeader indexFileHeader = null;
		try {
			indexFileHeader = getIndexFileHeader();
		} catch (IOException e) {
			indexFileHeader = new LogFileHeader(FILE_VERSION, LogFileHeader.MAGIC_STRING_INDEX);
		}
		os.write(indexFileHeader.serialize());
	}

	@Override
	public void writeDataFileHeader(OutputStream os) throws IOException {
		LogFileHeader dataFileHeader = null;
		try {
			dataFileHeader = getDataFileHeader();
		} catch (IOException e) {
			dataFileHeader = new LogFileHeader(FILE_VERSION, LogFileHeader.MAGIC_STRING_DATA);
			byte[] ext = new byte[4];
			int flags = 0;

			LogFileWriterV3o.prepareInt(flags, ext);
			if (compressionLevel > 0) {
				byte[] comp = compression.getBytes();
				ext = new byte[4 + comp.length];
				LogFileWriterV3o.prepareInt(flags, ext);
				ByteBuffer bb = ByteBuffer.wrap(ext, 4, comp.length);
				bb.put(comp);
			}

			dataFileHeader.setExtraData(ext);
		}
		os.write(dataFileHeader.serialize());
	}

	@Override
	public long getIndexFileHeaderLength() throws IOException {
		return getIndexFileHeader().size();
	}

	@Override
	public long getDataFileHeaderLength() throws IOException {
		return getDataFileHeader().size();
	}

	@Override
	public int getIndexBlockCount() throws IOException {
		if (!ifile.exists())
			return 0;

		return (int) ((getIndexFile().length() - getIndexFileHeaderLength()) / IndexBlockV3Header.ITEM_SIZE);
	}

	@Override
	public IndexBlockV3Header getIndexBlock(int id) throws IOException {
		StorageInputStream inputStream = null;
		try {
			IndexBlockV3Header unserializer = new IndexBlockV3Header();
			inputStream = ifile.newInputStream();

			inputStream.seek(getIndexFileHeaderLength() + IndexBlockV3Header.ITEM_SIZE * id);
			IndexBlockV3Header ret = unserializer.unserialize(id, inputStream);

			if (getIndexFileHeaderLength() + IndexBlockV3Header.ITEM_SIZE * (id + 1) >= getIndexFile().length()) {
				ret.setDataBlockLen(getDataFile().length() - ret.getPosOnData());
			} else {
				IndexBlockV3Header next = unserializer.unserialize(id + 1, inputStream);
				ret.setDataBlockLen(next.getPosOnData() - ret.getPosOnData());
			}

			return ret;
		} finally {
			StorageUtil.ensureClose(inputStream);
		}
	}

	@Override
	public RawDataBlock getRawDataBlock(IndexBlockV3Header indexBlock) throws IOException {
		ByteBuffer dataBuffer = ByteBuffer.allocate((int) indexBlock.getDataBlockLen());
		StorageInputStream stream = getDataStream();
		stream.seek(indexBlock.getPosOnData());
		stream.readFully(dataBuffer.array());
		return new RawDataBlock(indexBlock.getId(), dataBuffer);
	}

	public CloseableEnumeration<IndexBlockV3Header> getIndexBlocks() throws IOException {
		return super.getIndexBlocks();
	}

	public static class RawDataBlock extends org.araqne.storage.filepair.RawDataBlock<RawDataBlock> {
		private int id;
		private ByteBuffer blockBuffer;

		public RawDataBlock(int id, ByteBuffer blockBuffer) {
			this.id = id;
			this.blockBuffer = blockBuffer;
		}

		@Override
		public void serialize(OutputStream os) throws IOException {
			os.write(blockBuffer.array());
		}

		public int getId() {
			return id;
		}

		public String getDigest() {
			return calcHash(blockBuffer);
		}

	}

	@Override
	public int getIndexBlockSize() {
		return IndexBlockV3Header.ITEM_SIZE;
	}
}
