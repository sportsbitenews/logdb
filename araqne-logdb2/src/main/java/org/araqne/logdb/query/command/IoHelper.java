package org.araqne.logdb.query.command;

import java.io.Closeable;
import java.io.IOException;

public class IoHelper {

	public static void close(Closeable c) {
		if (c != null) {
			try {
				c.close();
			} catch (IOException e) {
			}
		}
	}
}
