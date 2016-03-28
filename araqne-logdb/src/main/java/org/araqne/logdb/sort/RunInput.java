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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RunInput {
	private static final int READ_BUFFER_SIZE = 1024 * 128;
	private final Logger logger = LoggerFactory.getLogger(RunInput.class);
	private Run run;
	public Iterator<Item> cachedIt;
	public BufferedInputStream bis;
	public Item loaded;

	private FileInputStream fis;
	private Item prefetch;
	private byte[] intbuf = new byte[4];
	private int loadCount;
	private AtomicInteger cacheCount;
	private final boolean decodeValue;

	public RunInput(Run run, AtomicInteger cacheCount, boolean decodeValue) throws IOException {
		this.run = run;
		this.cacheCount = cacheCount;
		this.decodeValue = decodeValue;

		if (run.cached != null) {
			cachedIt = run.cached.iterator();
		} else {
			this.fis = new FileInputStream(run.dataFile);
			if (run.offset > 0) {
				logger.debug("araqne logdb: run input #{}, offset #{}", run.id, run.offset);
				// index file must exists here
				long skip = readSkipLength(run.indexFile, run.offset);
				fis.skip(skip);
			}

			this.bis = new BufferedInputStream(fis, READ_BUFFER_SIZE);
		}
	}

	public int getId() {
		return run.id;
	}

	private long readSkipLength(File indexFile, int offset) throws IOException {
		RandomAccessFile raf = new RandomAccessFile(indexFile, "r");
		try {
			raf.seek(8 * offset);
			return raf.readLong();
		} finally {
			raf.close();
		}
	}

	public boolean hasNext() {
		if (loadCount >= run.length)
			return false;

		if (prefetch != null)
			return true;

		if (cachedIt != null) {
			if (cachedIt.hasNext()) {
				prefetch = cachedIt.next();
				if (prefetch.buf != null) {
					try {
						InputStream is = new ByteArrayInputStream(prefetch.buf);
						StreamEncodingRule.decode(is);
						prefetch.value = StreamEncodingRule.decode(is);
					} catch (IOException e) {
					}
				}
				return true;
			} else
				return false;
		}

		try {
			int readBytes = IoHelper.ensureRead(bis, intbuf, 4);
			if (readBytes == 4) {
				int len = IoHelper.decodeInt(intbuf);
				byte[] buf = new byte[len];

				readBytes = IoHelper.ensureRead(bis, buf, len);
				if (readBytes == len) {
					InputStream is = new ByteArrayInputStream(buf);
					Object key = StreamEncodingRule.decode(is);
					Object value = null;
					if (decodeValue) {
						value = StreamEncodingRule.decode(is);
						prefetch = new Item(key, value);
					} else {
						prefetch = new Item(key, buf, len);
					}
				}
			}
		} catch (IOException e) {
			logger.error("araqne logdb: cannot read run", e);
		}
		return prefetch != null;
	}

	public Item next() throws IOException {
		if (!hasNext())
			throw new NoSuchElementException();
		Item ret = prefetch;
		prefetch = null;
		loadCount++;
		return ret;
	}

	public void purge() {
		ensureClose(bis, fis);

		if (run.indexFile != null)
			run.indexFile.delete();

		if (run.dataFile != null)
			run.dataFile.delete();

		if (run.cached != null) {
			cacheCount.addAndGet(run.length);
		}
	}

	private void ensureClose(BufferedInputStream bis, FileInputStream fis) {
		try {
			if (bis != null)
				bis.close();
		} catch (IOException e) {
			logger.error("araqne logdb: cannot close run", e);
		}

		try {
			if (fis != null)
				fis.close();
		} catch (IOException e) {
			logger.error("araqne logdb: cannot close run", e);
		}
	}

}