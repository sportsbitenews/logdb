package org.araqne.logstorage.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import net.jpountz.util.Native;

public class Lz4Compression implements Compression {

	private static final LZ4Factory factory;

	static {
		org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(Lz4Compression.class);
		try {
			Native.load();
			slog.info("araqne logstorage: loaded native lz4 library successfully, result [{}]", Native.isLoaded());
		} catch (Throwable t) {
			slog.warn("araqne logstorage: cannot load native lz4 library, cause []", t.toString());
		} finally {
			factory = LZ4Factory.fastestInstance();
		}
	}

	@Override
	public ByteBuffer compress(byte[] b, int offset, int limit) throws IOException {
		LZ4Compressor compressor = factory.fastCompressor();
		int maxCompressedLength = compressor.maxCompressedLength(limit);
		byte[] compressed = new byte[maxCompressedLength];
		int compressedLength = compressor.compress(b, offset, limit, compressed, 0, maxCompressedLength);
		return ByteBuffer.wrap(Arrays.copyOf(compressed, compressedLength));
	}

	@Override
	public void uncompress(byte[] output, byte[] b, int offset, int limit) throws IOException {
		LZ4FastDecompressor decompressor = factory.fastDecompressor();
		decompressor.decompress(b, offset, output, 0, output.length);
	}

	@Override
	public void close() {
	}

}
