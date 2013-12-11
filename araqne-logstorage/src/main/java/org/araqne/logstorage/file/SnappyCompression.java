/*
 * Copyright 2013 Eediom Inc. All rights reserved.
 */
package org.araqne.logstorage.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.xerial.snappy.Snappy;

/**
 * @since 2.2.0
 * @author xeraph
 * 
 */
public class SnappyCompression implements Compression {
	@Override
	public ByteBuffer compress(byte[] b, int offset, int limit) throws IOException {
		ByteBuffer compressed = ByteBuffer.allocate(limit);
		int size = Snappy.compress(b, offset, limit, compressed.array(), 0);
		compressed = ByteBuffer.wrap(Arrays.copyOf(compressed.array(), size));
		return compressed;
	}

	@Override
	public void uncompress(byte[] output, byte[] b, int offset, int limit) throws IOException {
		Snappy.uncompress(b, offset, limit, output, 0);
	}

	@Override
	public void close() {
	}
}
