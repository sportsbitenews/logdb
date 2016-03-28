/*
 * Copyright 2012 Future Systems
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.araqne.logdb.sort;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RunInputRandomAccess {
	private final Logger logger = LoggerFactory.getLogger(RunInputRandomAccess.class);
	public Run run;
	private RandomAccessFile indexRaf;
	private RandomAccessFile dataRaf;
	private byte[] reuseBuffer = new byte[64 * 1024];

	public RunInputRandomAccess(Run run) throws IOException {
		this.run = run;

		if (run.cached == null) {
			// index file must exists here
			this.indexRaf = new RandomAccessFile(run.indexFile, "r");
			this.dataRaf = new RandomAccessFile(run.dataFile, "r");
		}
	}

	public Item get(long offset) throws IOException {
		if (offset >= run.length)
			throw new IndexOutOfBoundsException("offset=" + offset + ", run length=" + run.length);

		// anyway, you cannot cache more than int max
		if (run.cached != null)
			return run.cached.get((int) offset);

		indexRaf.seek(8 * offset);
		long pos = indexRaf.readLong();

		dataRaf.seek(pos);
		int len = dataRaf.readInt();

		byte[] b = IoHelper.ensureBuffer(reuseBuffer, len);
		int readBytes = dataRaf.read(b, 0, len);
		if (readBytes != len)
			throw new IOException("insufficient merge data block, expected=" + len + ", actual=" + readBytes);

		InputStream is = new ByteArrayInputStream(b);
		Object key = StreamEncodingRule.decode(is);
		Object value = StreamEncodingRule.decode(is);
		return new Item(key, value);
	}

	public void close() {
		if (indexRaf != null) {
			try {
				indexRaf.close();
			} catch (IOException e) {
				logger.error("araqne logdb: cannot close run", e);
			}
		}

		if (dataRaf != null) {
			try {
				dataRaf.close();
			} catch (IOException e) {
				logger.error("araqne logdb: cannot close run", e);
			}
		}
	}
}
