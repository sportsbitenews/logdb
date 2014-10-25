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
package org.araqne.storage.filepair;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import org.araqne.storage.api.FilePath;
import org.araqne.storage.api.StorageInputStream;
import org.araqne.storage.api.StorageOutputStream;
import org.araqne.storage.api.StorageUtil;
import org.araqne.storage.localfile.LocalFileOutputStream;
import org.araqne.storage.localfile.LocalFilePath;

public abstract class FilePair<IB extends IndexBlock<IB>, RDB extends RawDataBlock<RDB>> implements Closeable {
	protected final FilePath ifile;
	protected final FilePath dfile;
	protected final Class<IB> ibClass;
	protected final Class<RDB> rdbClass;

	public FilePair(FilePath indexFile, FilePath dataFile, Class<IB> ibClass, Class<RDB> rdbClass) {
		this.ifile = indexFile;
		this.dfile = dataFile;
		this.ibClass = ibClass;
		this.rdbClass = rdbClass;
	}

	public Class<? extends IndexBlock<?>> getIBClass() {
		return ibClass;
	}
	
	public Class<? extends RawDataBlock<?>> getRDBClass() {
		return rdbClass;
	}
	
	public FilePath getIndexFile() {
		return ifile;
	}

	public FilePath getDataFile() {
		return dfile;
	}
	
	public abstract void close() throws IOException;

	public static void main(String[] args) {
		FilePath a =
				new LocalFilePath("/works/araqne/latest_cache/data/araqne-logstorage/index/13/1/2014-06-27.1.bpos");
		System.out.println(getPathPart(
				"/works/araqne/latest_cache/data/araqne-logstorage/index/13/1/2014-06-27.1.bpos", 3));
		System.out.println(getPathPart("/araqne/latest_cache/data/araqne-logstorage/index/13/1/2014-06-27.1.bpos", 3));
		System.out.println(getPathPart("/latest_cache/data/araqne-logstorage/index/13/1/2014-06-27.1.bpos", 3));
		System.out.println(getPathPart("/data/araqne-logstorage/index/13/1/2014-06-27.1.bpos", 3));
		System.out.println(getPathPart("/araqne-logstorage/index/13/1/2014-06-27.1.bpos", 3));
		System.out.println(getPathPart("/index/13/1/2014-06-27.1.bpos", 3));
		System.out.println(getPathPart("/13/1/2014-06-27.1.bpos", 3));
		System.out.println(getPathPart("/1/2014-06-27.1.bpos", 3));
		System.out.println(getPathPart("/2014-06-27.1.bpos", 3));
		System.out.println(getPathPart("2014-06-27.1.bpos", 3));
		System.out.println(getPathPart("", 3));
		System.out.println(getPathPart("/", 3));
		System.out.println(getPathPart("//", 3));
		System.out.println(getPathPart("///", 3));
		System.out.println(getPathPart("/1/2014-06-27.1.bpos", 2));
		System.out.println(getPathPart("/1/2014-06-27.1.bpos/", 2));
	}

	private static String getPathPart(String absPath, int depth) {
		char ps = File.separatorChar;
		int startPos = absPath.length();
		for (int i = 0; i < depth; ++i) {
			int lastIndexOf = absPath.lastIndexOf(ps, startPos - 1);
			if (lastIndexOf == -1) {
				startPos = -1;
				break;
			} else {
				startPos = lastIndexOf;
			}
		}

		if (absPath.length() > 0 && absPath.charAt(startPos + 1) == ps)
			startPos += 1;

		return absPath.substring(startPos + 1);
	}

	@Override
	public String toString() {
		return String.format(
				"FilePair [ifile=%s, dfile=%s]", getPathPart(ifile.getAbsolutePath(), 3),
				getPathPart(dfile.getAbsolutePath(), 3));
	}

	public void ensureIndexFileHeader() throws IOException {
		if (ifile.isNotEmpty())
			return;

		ifile.getParentFilePath().mkdirs();
		StorageOutputStream stream = null;
		try {
			stream = ifile.newOutputStream(false);
			writeIndexFileHeader(stream);
		} finally {
			StorageUtil.ensureClose(stream);
		}
	}

	public void ensureDataFileHeader() throws IOException {
		if (dfile.isNotEmpty())
			return;

		dfile.getParentFilePath().mkdirs();
		StorageOutputStream stream = null;
		try {
			stream = dfile.newOutputStream(false);
			writeDataFileHeader(stream);
		} finally {
			StorageUtil.ensureClose(stream);
		}
	}

	public abstract void writeIndexFileHeader(OutputStream os) throws IOException;

	public abstract void writeDataFileHeader(OutputStream os) throws IOException;

	// may be same with body offset
	public abstract long getIndexFileHeaderLength() throws IOException;

	public abstract long getDataFileHeaderLength() throws IOException;

	public abstract int getIndexBlockCount() throws IOException;

	// assume index block size is fixed
	public abstract int getIndexBlockSize();

	public abstract IB getIndexBlock(int id) throws IOException;

	public CloseableEnumeration<IB> getIndexBlocks() throws IOException {
		return new IndexBlockEnumeration();
	}

	public abstract RDB getRawDataBlock(IB indexBlock) throws IOException;

	public void reserveBlocks(List<IB> addedBlocks) throws IOException {
		StorageOutputStream indexStream = null;
		StorageOutputStream dataStream = null;
		try {
			ensureIndexFileHeader();
			ensureDataFileHeader();

			indexStream = ifile.newOutputStream(true);
			dataStream = dfile.newOutputStream(true);

			long dflen = dfile.length();

			if (dataStream instanceof LocalFileOutputStream) {
				LocalFileOutputStream lDataStream = (LocalFileOutputStream) dataStream;
				for (IB block : addedBlocks) {
					IB reservedBlock = block.newReservedBlock();

					dflen = Math.max(dflen, reservedBlock.getPosOnData()) + reservedBlock.getDataBlockLen();
					lDataStream.setLength(dflen);
					reservedBlock.serialize(indexStream);
				}
			} else {
				throw new UnsupportedOperationException("the operation for non-local file is not supported yet");
			}
		} finally {
			StorageUtil.ensureClose(indexStream);
			StorageUtil.ensureClose(dataStream);
		}
	}

	public void replaceBlock(IB indexBlock, RDB rawDataBlock) throws IOException {
		IB reservedIndexBlock = getIndexBlock(indexBlock.getId());
		if (!reservedIndexBlock.isReserved())
			throw new IllegalArgumentException("the block is not reserved block");

		if (reservedIndexBlock.getPosOnData() != indexBlock.getPosOnData())
			throw new IllegalArgumentException("pos on data file is different");

		StorageOutputStream dataStream = null;
		StorageOutputStream indexStream = null;
		try {
			dataStream = dfile.newOutputStream(false);
			indexStream = ifile.newOutputStream(false);

			if (dataStream instanceof LocalFileOutputStream && indexStream instanceof LocalFileOutputStream) {
				LocalFileOutputStream lDataStream = (LocalFileOutputStream) dataStream;
				LocalFileOutputStream lIndexStream = (LocalFileOutputStream) indexStream;
				lDataStream.seek(indexBlock.getPosOnData());
				rawDataBlock.serialize(lDataStream);

				lIndexStream.seek(getIndexFileHeaderLength() + indexBlock.getBlockSize() * indexBlock.getId());
				indexBlock.serialize(lIndexStream);
			} else {
				throw new UnsupportedOperationException("the operation for non-local file is not supported yet");
			}

		} finally {
			StorageUtil.ensureClose(dataStream);
			StorageUtil.ensureClose(indexStream);
		}

	}

	public void addBlock(IB indexBlock, RDB rawDataBlock) throws IOException {
		if (indexBlock.getId() != getIndexBlockCount())
			throw new CannotAppendBlockException(this + ": unexpected block id - " + indexBlock.getId() + " (total: "
					+ getIndexBlockCount() + ")");

		StorageOutputStream dataStream = null;
		StorageOutputStream indexStream = null;
		try {
			ensureIndexFileHeader();
			ensureDataFileHeader();

			if (indexBlock.getPosOnData() != dfile.length())
				throw new CannotAppendBlockException(this + ": unexpected data block position " + indexBlock.getPosOnData()
						+ " (expected: " + dfile.length() + ")");

			long indexPos = getIndexFileHeaderLength() + indexBlock.getBlockSize() * indexBlock.getId();
			if (indexPos != ifile.length())
				throw new CannotAppendBlockException(this + ": unexpected index block position" + indexPos + " (expected: "
						+ ifile.length() + ")");

			dataStream = dfile.newOutputStream(true);
			rawDataBlock.serialize(dataStream);

			indexStream = ifile.newOutputStream(true);
			indexBlock.serialize(indexStream);
		} finally {
			StorageUtil.ensureClose(dataStream);
			StorageUtil.ensureClose(indexStream);
		}
	}

	public void truncate(int remainingBlockCount) throws IOException {
		if (remainingBlockCount < 0)
			throw new IllegalArgumentException(this + ": block count cannot be negative");

		if (getIndexBlockCount() == 0)
			return;

		long ilen = getIndexFileHeaderLength() + getIndexBlockSize() * remainingBlockCount;
		if (ifile.length() <= ilen)
			return;

		IB firstRemoveBlock = getIndexBlock(remainingBlockCount);
		long dlen = firstRemoveBlock.getPosOnData();

		StorageOutputStream dataStream = null;
		StorageOutputStream indexStream = null;
		try {
			dataStream = dfile.newOutputStream(false);
			indexStream = ifile.newOutputStream(false);

			if (dataStream instanceof LocalFileOutputStream && indexStream instanceof LocalFileOutputStream) {
				LocalFileOutputStream lDataStream = (LocalFileOutputStream) dataStream;
				LocalFileOutputStream lIndexStream = (LocalFileOutputStream) indexStream;
				lIndexStream.setLength(ilen);
				lDataStream.setLength(dlen);
			} else {
				throw new UnsupportedOperationException("the operation for non-local file is not supported yet");
			}

		} finally {
			StorageUtil.ensureClose(dataStream);
			StorageUtil.ensureClose(indexStream);
		}
	}

	public static String calcHash(ByteBuffer bb) {
		ByteBuffer buf = bb.asReadOnlyBuffer();
		try {
			if (buf.hasArray()) {
				try {
					MessageDigest md5 = MessageDigest.getInstance("MD5");
					md5.digest(buf.array(), buf.position(), buf.remaining());

					return String.format("%X", new BigInteger(1, md5.digest()));
				} catch (DigestException e) {
					return null;
				}
			} else {
				MessageDigest md5 = MessageDigest.getInstance("MD5");
				ByteBuffer b = buf.asReadOnlyBuffer();
				byte[] array = new byte[b.remaining()];
				b.get(array);
				return String.format("%X", new BigInteger(1, md5.digest(array)));
			}
		} catch (NoSuchAlgorithmException e1) {
			return null;
		}
	}

	private class IndexBlockEnumeration implements CloseableEnumeration<IB> {
		private StorageInputStream stream;
		private long fileLength;
		private long dataStreamLength;
		private long segCount;
		private int currentSegId;
		private IB next;
		private IB prefetched;
		private IB ib;

		public IndexBlockEnumeration() throws IOException {
			this.currentSegId = 0;

			try {
				this.ib = ibClass.newInstance();

				if (!ifile.exists() || ifile.length() == 0)
					return;

				this.stream = ifile.newInputStream();
				this.dataStreamLength = dfile.length();
				this.fileLength = stream.length();
				this.segCount = (fileLength - getIndexFileHeaderLength()) / ib.getBlockSize();

				this.stream.seek(getIndexFileHeaderLength());

			} catch (InstantiationException e) {
				throw new IllegalArgumentException("ibClass is not class instance of IB", e);
			} catch (IllegalAccessException e) {
				throw new IllegalArgumentException("ibClass is not class instance of IB", e);
			}
		}

		@Override
		public boolean hasMoreElements() {
			if (this.stream == null)
				return false;

			if (next != null)
				return true;

			if (currentSegId == segCount)
				return false;

			try {
				if (currentSegId == 0) {
					ByteBuffer b = ByteBuffer.allocate(ib.getBlockSize());
					readIndexBlock(b);

					IB newInstance = (IB) ibClass.newInstance();
					next = newInstance.unserialize(currentSegId, new ByteArrayInputStream(b.array()));
				} else {
					next = prefetched;
				}

				if (currentSegId < segCount - 1) {
					ByteBuffer b = ByteBuffer.allocate(ib.getBlockSize());
					readIndexBlock(b);
					prefetched =
							ibClass.newInstance().unserialize(currentSegId + 1, new ByteArrayInputStream(b.array()));
					next.setDataBlockLen(prefetched.getPosOnData() - next.getPosOnData());
				} else {
					next.setDataBlockLen(dataStreamLength - next.getPosOnData());
				}

				boolean hasNext = currentSegId < segCount;

				currentSegId += 1;

				return hasNext;
			} catch (Throwable t) {
				throw new IllegalStateException(t);
			}
		}

		@Override
		public IB nextElement() {
			IB next = this.next;

			this.next = null;

			return next;
		}

		private void readIndexBlock(ByteBuffer pfb) throws IOException {
			this.stream.readBestEffort(pfb);
			pfb.flip();
			pfb.order(ByteOrder.BIG_ENDIAN);
		}

		@Override
		public void close() throws IOException {
			StorageUtil.ensureClose(this.stream);
			this.stream = null;
		}
	}
}
