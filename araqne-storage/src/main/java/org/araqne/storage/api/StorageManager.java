package org.araqne.storage.api;

public interface StorageManager {
	/**
	 * @param URI
	 *            File URI
	 * @return FilePath Object for given URI
	 * 		   if manager can't handle URI, return null
	 */
	FilePath resolveFilePath(String path);
	
	void start();
	
	void stop();
}
