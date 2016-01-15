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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.araqne.storage.api.StorageUtil;
import org.araqne.storage.filepair.IndexBlock;

public class IndexBlockV3Header extends IndexBlock<IndexBlockV3Header>{
	public static final int ITEM_SIZE = 28;
	private int id;
	
	private boolean isReserved;
	
	// index header file offset
	public long fp;
	public long firstId;

	public long dataFp;
	public long minTime;
	public long maxTime;
	public int logCount;

	// except this block's log count
	public long ascLogCount;
	public long dscLogCount;
	private Long dataBlockLen;

	// for unserialize
	public IndexBlockV3Header() {
	}
	
	public IndexBlockV3Header(int id, long datafp, long minTime, long maxTime, int logCount, long firstId) {
		this(id, datafp, minTime, maxTime, logCount, firstId, datafp < 0);
	}
	
	public IndexBlockV3Header(int id, long datafp, long minTime, long maxTime, int logCount, long firstId, boolean isReserved) {
		this.id = id;
		this.isReserved = isReserved;
		this.dataFp = Math.abs(datafp);
		this.minTime = minTime;
		this.maxTime = maxTime;
		this.logCount = logCount;
		this.firstId = firstId;
	}

	@Override
	public String toString() {
		return "index block header, fp=" + fp + ", first_id=" + firstId + ", count=" + logCount + ", asc=" + ascLogCount
				+ ", dsc=" + dscLogCount + "]";
	}

	@Override
	public int getId() {
		return id;
	}

	@Override
	public boolean isReserved() {
		return isReserved;
	}

	@Override
	public long getPosOnData() {
		return dataFp;
	}

	@Override
	public int getBlockSize() {
		return ITEM_SIZE;
	}

	@Override
	public void serialize(OutputStream os) throws IOException {
		byte[] longbuf = new byte[8];
		byte[] intbuf = new byte[4];
		
		long datFp = isReserved? (-1 * dataFp): dataFp;
		prepareLong(datFp, longbuf);
		os.write(longbuf);
		prepareLong(minTime, longbuf);
		os.write(longbuf);
		prepareLong(maxTime, longbuf);
		os.write(longbuf);
		prepareInt(logCount, intbuf);
		os.write(intbuf);
	}

	@Override
	public IndexBlockV3Header unserialize(int blockId, InputStream is) throws IOException {
		ByteBuffer bb = ByteBuffer.allocate(IndexBlockV3Header.ITEM_SIZE);
		StorageUtil.readFully(is, bb);
		return unserialize(blockId, bb);
	}
	
	@Override
	public IndexBlockV3Header unserialize(int blockId, ByteBuffer buf) {
		long dataFp = buf.getLong();
		boolean isReserved = dataFp < 0;
		return new IndexBlockV3Header(blockId, Math.abs(dataFp), buf.getLong(), buf.getLong(), buf.getInt(), -1, isReserved);
	}

	@Override
	public Long getDataBlockLen() {
		return dataBlockLen;
	}

	@Override
	public IndexBlockV3Header newReservedBlock() {
		IndexBlockV3Header ret = new IndexBlockV3Header(id, dataFp, minTime, maxTime, logCount, firstId, true);
		ret.setDataBlockLen(getDataBlockLen());
		return ret;
	}

	@Override
	public void setDataBlockLen(long dataBlockLen) {
		this.dataBlockLen = dataBlockLen;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (ascLogCount ^ (ascLogCount >>> 32));
		result = prime * result + ((dataBlockLen == null) ? 0 : dataBlockLen.hashCode());
		result = prime * result + (int) (dataFp ^ (dataFp >>> 32));
		result = prime * result + (int) (dscLogCount ^ (dscLogCount >>> 32));
		result = prime * result + (int) (firstId ^ (firstId >>> 32));
		result = prime * result + (int) (fp ^ (fp >>> 32));
		result = prime * result + id;
		result = prime * result + (isReserved ? 1231 : 1237);
		result = prime * result + logCount;
		result = prime * result + (int) (maxTime ^ (maxTime >>> 32));
		result = prime * result + (int) (minTime ^ (minTime >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IndexBlockV3Header other = (IndexBlockV3Header) obj;
		if (ascLogCount != other.ascLogCount)
			return false;
		if (dataBlockLen == null) {
			if (other.dataBlockLen != null)
				return false;
		} else if (!dataBlockLen.equals(other.dataBlockLen))
			return false;
		if (dataFp != other.dataFp)
			return false;
		if (dscLogCount != other.dscLogCount)
			return false;
		if (firstId != other.firstId)
			return false;
		if (fp != other.fp)
			return false;
		if (id != other.id)
			return false;
		if (isReserved != other.isReserved)
			return false;
		if (logCount != other.logCount)
			return false;
		if (maxTime != other.maxTime)
			return false;
		if (minTime != other.minTime)
			return false;
		return true;
	}

	@Override
	public boolean isEquivalent(IndexBlockV3Header obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IndexBlockV3Header other = (IndexBlockV3Header) obj;
		if (id != other.id)
			return false;
		if (minTime != other.minTime)
			return false;
		if (maxTime != other.maxTime)
			return false;
		if (dataFp != other.dataFp)
			return false;
		if (dataBlockLen == null || other.dataBlockLen == null) {
			throw new IllegalArgumentException("not nullable item: dataBlockLen");
		} else if (!dataBlockLen.equals(other.dataBlockLen))
			return false;
		return true;
	}

	static void prepareInt(int l, byte[] b) {
		for (int i = 0; i < 4; i++)
			b[i] = (byte) ((l >> ((3 - i) * 8)) & 0xff);
	}

	static void prepareLong(long l, byte[] b) {
		for (int i = 0; i < 8; i++)
			b[i] = (byte) ((l >> ((7 - i) * 8)) & 0xff);
	}

}
