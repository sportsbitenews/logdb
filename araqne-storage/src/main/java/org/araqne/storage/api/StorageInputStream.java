package org.araqne.storage.api;

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface StorageInputStream extends Closeable, DataInput {
	void readFully(ByteBuffer buf, long startPos) throws IOException;
	
	int read(byte[] b) throws IOException;
	
	int read(byte[] b, int off, int len) throws IOException;
	
	long length() throws IOException;
	
	void seek(long pos) throws IOException;
	
	FilePath getPath();
}
