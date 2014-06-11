package org.araqne.storage.api;

public interface StorageManager {
	/**
	 * @param URI
	 *            File URI
	 * @return FilePath Object for given URI
	 * 		   if manager can't handle URI, return null
	 */
	FilePath resolveFilePath(String path);
	
	/**
	 * @param URIResolver
	 *			UriResolver make FilePath Object for specific URI
	 */
	void addURIResolver(URIResolver r);
	
	void start();
	
	void stop();
}
