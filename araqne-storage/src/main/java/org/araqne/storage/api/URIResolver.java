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
	 * @return Array of Protocol String that UriResolver can handle
	 */
	String[] getProtocols();
}
