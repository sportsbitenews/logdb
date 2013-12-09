package org.araqne.storage.api;

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface StorageInput  extends Closeable, DataInput {
	FilePath getPath();

	long length() throws IOException;
	
	void seek(long pos) throws IOException;
	
	void readFully(ByteBuffer buf, long startPos) throws IOException;
	
}
