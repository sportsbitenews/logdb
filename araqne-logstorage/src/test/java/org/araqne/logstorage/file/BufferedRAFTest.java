package org.araqne.logstorage.file;

import static org.junit.Assert.assertArrayEquals;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

public class BufferedRAFTest {
	@Test
	public void readSizeTest() throws IOException {
		File file = new File("braf_test.dat");
		try {
			BufferedOutputStream s = new BufferedOutputStream(new FileOutputStream(file));
			Random rand = new Random(1);
			final int BUFSIZE = 16384;
			byte[] buf = new byte[BUFSIZE];
			for (int i = 0; i < 100; ++i) {
				rand.nextBytes(buf);
				s.write(buf);
			}
			s.close();

			byte[] diffsrc = new byte[BUFSIZE * 100];
			new Random(1).nextBytes(diffsrc);

			BufferedRandomAccessFileReader reader = new BufferedRandomAccessFileReader(file);
			assertArrayEquals(subarray(diffsrc, 0, 1024), readRAF(reader, 0, 1024));
			assertArrayEquals(subarray(diffsrc, 0, 8192 * 4), readRAF(reader, 0, 8192 * 4));
			assertArrayEquals(subarray(diffsrc, 8192 * 15, 8192 * 4), readRAF(reader, 8192 * 15, 8192 * 4));

		} finally {
			file.delete();
		}

	}

	private byte[] subarray(byte[] diffsrc, int offset, int length) {
		return Arrays.copyOfRange(diffsrc, offset, offset + length);
	}

	private byte[] readRAF(BufferedRandomAccessFileReader reader, int seekpos, int length) throws IOException {
		byte[] ret = new byte[length];
		reader.seek(seekpos);
		reader.readFully(ret);
		return ret;
	}
}
