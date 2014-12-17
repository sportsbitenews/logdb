package org.araqne.storage.api;

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface StorageInput  extends Closeable, DataInput {
	FilePath getPath();

	long length() throws IOException;
	
	void seek(long pos) throws IOException;
	
	long getPos() throws IOException;
	
	/**
	 * read data to buffer best effort. This method is not thread-safe.
	 * @param buf
	 * @throws IOException
	 */
	int readBestEffort(ByteBuffer buf) throws IOException;
	
	void sync() throws IOException;
}
