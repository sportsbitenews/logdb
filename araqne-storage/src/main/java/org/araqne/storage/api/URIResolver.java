package org.araqne.storage.api;

public interface URIResolver {
	/**
	 * @param URI
	 *            File URI
	 * @return FilePath Object for given URI
	 * 		   if manager can't handle URI, return null
	 */
	FilePath resolveFilePath(String path);
	
	/**
	 * @return Protocol String UriResolver can handle
	 */
	String getProtocol();
}
