package org.araqne.logstorage.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

public class Lz4HcCompression implements Compression {

	@Override
	public ByteBuffer compress(byte[] b, int offset, int limit) throws IOException {
		LZ4Factory factory = LZ4Factory.fastestInstance();
		LZ4Compressor compressor = factory.highCompressor();
		int maxCompressedLength = compressor.maxCompressedLength(limit);
		byte[] compressed = new byte[maxCompressedLength];
		int compressedLength = compressor.compress(b, offset, limit, compressed, 0, maxCompressedLength);
		return ByteBuffer.wrap(Arrays.copyOf(compressed, compressedLength));
	}

	@Override
	public void uncompress(byte[] output, byte[] b, int offset, int limit) throws IOException {
		LZ4Factory factory = LZ4Factory.fastestInstance();
		LZ4FastDecompressor decompressor = factory.fastDecompressor();
		decompressor.decompress(b, offset, output, 0, output.length);
	}

	@Override
	public void close() {
	}

}
