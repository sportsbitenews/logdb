package org.araqne.storage.api;

import java.io.IOException;

public interface FilePath extends Comparable<FilePath> {
	String getProtocol();
	
	String getAbsolutePath() throws SecurityException;
	
	String getCanonicalPath() throws IOException, SecurityException;
	
	String getName();
	
	boolean exists() throws SecurityException;
	
	boolean isNotEmpty() throws IOException;
	
	boolean mkdirs() throws SecurityException;
	
	boolean delete() throws SecurityException;
	
	boolean deleteOnExit() throws SecurityException;
	
	boolean renameTo(FilePath dest) throws SecurityException;
	
	boolean isDirectory() throws SecurityException;
	
	boolean isFile() throws SecurityException;
	
	boolean canRead() throws SecurityException;
	
	boolean canWrite() throws SecurityException;
	
	char getSeperatorChar();
	
	long length() throws SecurityException;
	
	FilePath[] listFiles() throws SecurityException;
	
	FilePath[] listFiles(FilePathNameFilter filter) throws SecurityException;

	StorageInputStream newInputStream() throws IOException;
	
	StorageOutputStream newOutputStream(boolean append) throws IOException;
	
	FilePath newFilePath(String child);
	
	FilePath createTempFilePath(String prefix, String suffix) throws IOException, IllegalArgumentException, SecurityException;
	
	FilePath getParentFilePath() throws SecurityException;
	
	FilePath getAbsoluteFilePath() throws SecurityException;
	
	long getFreeSpace() throws SecurityException;
	
	long getUsableSpace() throws SecurityException;
	
	long getTotalSpace() throws SecurityException;

	long lastModified() throws SecurityException, IOException;
}
