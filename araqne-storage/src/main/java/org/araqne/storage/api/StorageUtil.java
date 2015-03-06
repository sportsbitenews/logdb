package org.araqne.storage.api;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

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
	
	public static void readFully(InputStream in, ByteBuffer bb) throws IOException {
		int total = 0;
		int limit = bb.limit();
		while (total < limit) {
			int ret = in.read(bb.array(), total, limit - total);
			if (ret < 0)
				throw new IOException("ByteBuffer underflow");
			total += ret;
		}
	}
	
	public static boolean deleteRecursive(FilePath p) {
		if (p.isDirectory()) {
			for (FilePath f : p.listFiles()) {
				deleteRecursive(f);
			}
		}
		
		return p.delete();
	}

}
