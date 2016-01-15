package org.araqne.storage.filepair;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public abstract class IndexBlock<Derived extends IndexBlock<Derived>> {
	public abstract int getId();
	
	public abstract boolean isEquivalent(Derived obj);

	public abstract boolean isReserved();

	public abstract long getPosOnData();
	
	public boolean hasEndPosOnData() { return false; }
	
	public long getEndPosOnData() { return -1; }

	public abstract int getBlockSize();

	public abstract void serialize(OutputStream os) throws IOException;

	public abstract Derived unserialize(int blockId, InputStream os) throws IOException;

	public abstract Derived unserialize(int blockId, ByteBuffer buf);

	public abstract Long getDataBlockLen();

	public abstract Derived newReservedBlock();
	
	public abstract void setDataBlockLen(long dataBlockLen);
}
