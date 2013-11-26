package org.araqne.storage.api;

import java.io.Closeable;
import java.io.DataOutput;
import java.io.IOException;

public interface StorageOutputStream extends Closeable, DataOutput {
	FilePath getPath();
	
	void sync() throws IOException;
}
