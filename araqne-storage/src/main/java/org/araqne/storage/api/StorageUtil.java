package org.araqne.storage.api;

import java.io.Closeable;
import java.io.IOException;

public class StorageUtil {
	public static String extractProtocol(String path) {
		int index = path.indexOf("://");
		if (index < 0)
			return null;
		
		return path.substring(0, index + 3);
	}
	
	public static void ensureClose(Closeable c) {
		if (c != null) {
			try {
				c.close();
			} catch (IOException e) {
			}
		}
	}
}
