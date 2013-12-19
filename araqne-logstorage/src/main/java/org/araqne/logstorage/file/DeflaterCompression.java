/*
 * Copyright 2013 Eediom Inc. All rights reserved.
 */
package org.araqne.logstorage.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * @since 2.2.0
 * @author xeraph
 * 
 */
public class DeflaterCompression implements Compression {

	private Deflater deflater;
	private Inflater inflater = new Inflater();

	public DeflaterCompression() {
		this(3);
	}

	public DeflaterCompression(int level) {
		this.deflater = new Deflater(level);
	}

	@Override
	public ByteBuffer compress(byte[] b, int offset, int limit) throws IOException {
		Deflater c = deflater;
		c.reset();
		c.setInput(b, offset, limit);
		c.finish();
		ByteBuffer compressed = ByteBuffer.allocate(limit);
		int compressedSize = c.deflate(compressed.array());
		compressed = ByteBuffer.wrap(Arrays.copyOf(compressed.array(), compressedSize));
		return compressed;
	}

	@Override
	public void uncompress(byte[] output, byte[] b, int offset, int limit) throws IOException {
		inflater.setInput(b, offset, limit);
		try {
			inflater.inflate(output);
			inflater.reset();
		} catch (DataFormatException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void close() {
		deflater.end();
		inflater.end();
	}

}
