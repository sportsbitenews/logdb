package org.araqne.storage.api;

public class StorageUtil {
	public static String extractProtocol(String path) {
		int index = path.indexOf("://");
		if (index < 0)
			return null;
		
		return path.substring(0, index + 3);
	}
}
