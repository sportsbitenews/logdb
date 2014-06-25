package org.araqne.log.api;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class WtmpEntryParser {

	public abstract WtmpEntry parseEntry(ByteBuffer bb) throws IOException;

	public abstract int getBlockSize();

	/**
	 * read null terminated string
	 * 
	 * @param b
	 *            byte buffer
	 * @return the string
	 */
	public static String readString(byte[] b) {
		int i = 0;
		while (i < b.length && b[i] != 0)
			i++;

		return new String(b, 0, i);
	}
}
