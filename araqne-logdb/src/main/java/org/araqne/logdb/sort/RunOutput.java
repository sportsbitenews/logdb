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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.araqne.codec.FastEncodingRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RunOutput {
	private static final int WRITE_BUFFER_SIZE = 1024 * 1024 * 8;
	private final Logger logger = LoggerFactory.getLogger(RunOutput.class);
	public BufferedOutputStream dataBos;

	private Run run;
	private BufferedOutputStream indexBos;
	private FileOutputStream indexFos;
	private FileOutputStream dataFos;
	private byte[] intbuf = new byte[4];
	private byte[] longbuf = new byte[8];
	private long dataOffset;
	private boolean noIndexWrite;
	private FastEncodingRule enc = new FastEncodingRule();

	public RunOutput(int id, int length, AtomicInteger cacheCount, String tag) throws IOException {
		this(id, length, cacheCount, false, tag);
	}

	public RunOutput(int id, int length, AtomicInteger cacheCount, boolean noIndexWrite, String tag) throws IOException {
		this.noIndexWrite = noIndexWrite;

		int remainCacheSize = cacheCount.addAndGet(-length);
		if (remainCacheSize >= 0) {
			this.run = new Run(id, new LinkedList<Item>());
		} else {
			cacheCount.addAndGet(length);

			File indexFile = null;
			String araDataDir = System.getProperty("araqne.sort.dir", System.getProperty("araqne.data.dir"));
			File tmpDir = new File(araDataDir, "araqne-logdb/sort");
			tmpDir.mkdirs();
			File dataFile = File.createTempFile("run" + tag, ".dat", tmpDir);
			if (!noIndexWrite) {
				indexFile = File.createTempFile("run" + tag, ".idx", tmpDir);
				logger.debug("araqne logdb: creating run output index [{}]", indexFile.getAbsolutePath());
				indexFos = new FileOutputStream(indexFile);
				indexBos = new BufferedOutputStream(indexFos, WRITE_BUFFER_SIZE);
			}
			dataFos = new FileOutputStream(dataFile);
			dataBos = new BufferedOutputStream(dataFos, WRITE_BUFFER_SIZE);

			ReferenceCountedFile rcIndex = null;
			if (indexFile != null)
				rcIndex = new ReferenceCountedFile(indexFile.getAbsolutePath());

			ReferenceCountedFile rcData = new ReferenceCountedFile(dataFile.getAbsolutePath());

			this.run = new Run(id, length, rcIndex, rcData);
		}
	}

	public void write(List<Item> items) throws IOException {
		if (run.cached != null)
			run.cached.addAll(items);
		else {
			for (Item o : items)
				writeEntry(o);
		}
	}

	public void write(Item o) throws IOException {
		if (run.cached != null)
			run.cached.add(o);
		else {
			writeEntry(o);
		}
	}

	private void writeEntry(Item o) throws IOException {
		ByteBuffer buf = enc.encode(o, SortCodec.instance);
		int len = buf.remaining();

		if (!noIndexWrite) {
			IoHelper.encodeLong(longbuf, dataOffset);
			indexBos.write(longbuf);
		}

		IoHelper.encodeInt(intbuf, len);
		dataBos.write(intbuf);
		dataBos.write(buf.array(), 0, len);
		buf.clear();

		dataOffset += 4 + len;
	}

	public Run finish() {
		ensureClose(indexBos, indexFos);
		ensureClose(dataBos, dataFos);

		run.updateLength();
		return run;
	}

	private void ensureClose(BufferedOutputStream bos, FileOutputStream fos) {
		if (bos != null) {
			try {
				bos.close();
			} catch (IOException e) {
				logger.error("araqne logdb: cannot close run output", e);
			}
		}

		if (fos != null) {
			try {
				fos.close();
			} catch (IOException e) {
				logger.error("araqne logdb: cannot close run output", e);
			}
		}
	}
}
