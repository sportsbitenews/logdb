package org.araqne.storage.filepair;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public abstract class IndexBlock<Derived extends IndexBlock<Derived>> {
	public abstract int getId();

	public abstract boolean isReserved();

	public abstract long getPosOnData();

	public abstract int getBlockSize();

	public abstract void serialize(OutputStream os) throws IOException;

	public abstract Derived unserialize(int blockId, InputStream os) throws IOException;

	public abstract long getDataBlockLen();

	public abstract Derived newReservedBlock();
	
	public abstract void setDataBlockLen(long dataBlockLen);
}
