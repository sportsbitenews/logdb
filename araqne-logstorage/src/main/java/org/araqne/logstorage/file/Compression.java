/*
 * Copyright 2013 Eediom Inc. All rights reserved.
 */
package org.araqne.logstorage.file;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @since 2.2.0
 * @author xeraph
 * 
 */
public interface Compression {
	ByteBuffer compress(byte[] b, int offset, int limit) throws IOException;

	void uncompress(byte[] output, byte[] b, int offset, int limit) throws IOException;
	
	void close();
}
