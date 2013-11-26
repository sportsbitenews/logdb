package org.araqne.storage.api;

import java.io.IOException;

public interface FilePath {
	String getAbsolutePath();
	
	String getName();
	
	boolean exists();
	
	boolean mkdirs();
	
	boolean delete();
	
	boolean isDirectory();
	
	boolean isFile();
	
	long length();
	
	FilePath[] listFiles();
	
	FilePath[] listFiles(FilePathNameFilter filter);

	StorageInputStream newInputStream() throws IOException;
	
	StorageOutputStream newOutputStream(boolean append) throws IOException;
	
	FilePath newFilePath(String child);
	
	FilePath getParentFilePath();
	
	long getUsableSpace();
	
	long getTotalSpace();
}
