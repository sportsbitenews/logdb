package org.araqne.storage.api;

import java.io.IOException;

public interface FilePath extends Comparable<FilePath> {
	String getProtocol();
	
	String getAbsolutePath();
	
	String getCanonicalPath() throws IOException;
	
	String getName();
	
	boolean exists();
	
	boolean mkdirs();
	
	boolean delete();
	
	boolean renameTo(FilePath dest);
	
	boolean isDirectory();
	
	boolean isFile();
	
	boolean canRead();
	
	boolean canWrite();
	
	char getSeperatorChar();
	
	long length();
	
	FilePath[] listFiles();
	
	FilePath[] listFiles(FilePathNameFilter filter);

	StorageInputStream newInputStream() throws IOException;
	
	StorageOutputStream newOutputStream(boolean append) throws IOException;
	
	FilePath newFilePath(String child);
	
	FilePath createTempFilePath(String prefix, String suffix) throws IOException;
	
	FilePath getParentFilePath();
	
	FilePath getAbsoluteFilePath();
	
	long getUsableSpace();
	
	long getTotalSpace();
}
