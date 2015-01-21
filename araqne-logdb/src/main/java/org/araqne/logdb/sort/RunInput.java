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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import org.araqne.codec.EncodingRule;
import org.araqne.logstorage.file.BufferedStorageInputStream;
import org.araqne.storage.api.FilePath;
import org.araqne.storage.localfile.LocalFilePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RunInput {
	private static final int READ_BUFFER_SIZE = 1024 * 128;
	private final Logger logger = LoggerFactory.getLogger(RunInput.class);
	private ThreadLocal<byte[]> readBuffer = new ThreadLocal<byte[]>() {
		@Override
		protected byte[] initialValue() {
			return new byte[640 * 1024];
		}
	};

	private Run run;
	public Iterator<Item> cachedIt;
	public Item loaded;

	private Item prefetch;
	private byte[] intbuf = new byte[4];
	private int loadCount;
	private AtomicInteger cacheCount;
	private Item prefetchFirstItem;
	private Item prefetchLastItem;

	private BufferedStorageInputStream indexFile;
	private BufferedStorageInputStream dataFile;
	private BufferedStorageInputStream indexFile2;
	private BufferedStorageInputStream dataFile2;

	public RunInput(Run run, AtomicInteger cacheCount) throws IOException {
		this.run = run;
		if (run.dataFile != null)
			run.dataFile.share();
		if (run.indexFile != null)
			run.indexFile.share();

		this.cacheCount = cacheCount;

		if (run.cached != null) {
			cachedIt = run.cached.iterator();
		} else {
			FilePath indexPath = new LocalFilePath(run.indexFile);
			FilePath dataPath = new LocalFilePath(run.dataFile);
			this.indexFile = new BufferedStorageInputStream(indexPath);
			this.dataFile = new BufferedStorageInputStream(dataPath);
			this.indexFile2 = new BufferedStorageInputStream(indexPath);
			this.dataFile2 = new BufferedStorageInputStream(dataPath);

			indexFile.seek(run.offset * LONG_SIZE);
			long dataOffset = indexFile.readLong();
			dataFile.seek(dataOffset);
		}
	}

	public int getId() {
		return run.id;
	}

	public Item getFirstItem() throws IOException {
		// TODO
		// Load First and Last in one read of data and index file
		// Currently read 2 times.
		if (prefetchFirstItem == null)
			prefetchFirstItem = this.getItem(0);

		return this.prefetchFirstItem;
	}

	public Item getLastItem() throws IOException {
		// TODO
		// Load First and Last in one read of data and index file
		// Currently read 2 times.
		if (prefetchLastItem == null)
			prefetchLastItem = this.getItem(run.length - 1);

		return this.prefetchLastItem;
	}

	private static int LONG_SIZE = 8;

	public Item getItem(int nth) throws IOException {
		if (run.cached != null) {
			return run.cached.get(nth);
		} else {
			int indexOffset = (run.offset + nth) * LONG_SIZE;
			indexFile2.seek(indexOffset);
			long dataOffset = indexFile2.readLong();

			dataFile2.seek(dataOffset);
			int len = dataFile2.readInt();

			byte[] buf = new byte[len];
			dataFile2.read(buf);
			Item item = (Item) EncodingRule.decode(ByteBuffer.wrap(buf, 0, len), SortCodec.instance);

			return item;
		}
	}

	int lowerBound(int low, int high, Item item, Comparator<Item> comparator) throws IOException
	{
		if (low < 0)
			return 0;
		if (low >= high)
		{
			Item lowItem = getItem(low);
			int compareResult = comparator.compare(lowItem, item);

			if (compareResult >= 0)
				return low;
			else
				return low + 1;
		} else {
			int mid = (low + high) / 2;
			Item midItem = getItem(mid);
			int compareResult = comparator.compare(midItem, item);
			
			if (compareResult < 0) 
				return lowerBound(mid + 1, high, item, comparator);
			return lowerBound(low, mid, item, comparator);
		}

	}

	public void search(Item item, Comparator<Item> comparator) throws IOException {
		int nth = lowerBound(loadCount, run.length-1, item, comparator);
		if(nth == run.length)
			setNext(run.length-1);
		else 
			setNext(nth);
	}

	private void setNext(int nth) throws IOException {
		if (cachedIt != null) {
			prefetch = null;
			cachedIt = run.cached.listIterator(nth);
		} else {
			int indexOffset = (run.offset + nth) * LONG_SIZE;
			loadCount = nth;

			indexFile.seek(indexOffset);
			long dataOffset = indexFile.readLong();
			dataFile.seek(dataOffset);

			prefetch = null;
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
				return true;
			} else
				return false;
		}

		try {
			int len = dataFile.readInt();
			byte[] temp = new byte[len];
			dataFile.read(temp);
			prefetch = (Item) EncodingRule.decode(ByteBuffer.wrap(temp, 0, len), SortCodec.instance);
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
		ensureClose(indexFile);
		ensureClose(dataFile);
		ensureClose(indexFile2);
		ensureClose(dataFile2);

		if (run.indexFile != null)
			run.indexFile.delete();

		if (run.dataFile != null)
			run.dataFile.delete();

		if (run.cached != null) {
			cacheCount.addAndGet(run.length);
		}
	}

	private void ensureClose(BufferedStorageInputStream file) {
		try {
			if (file != null)
				file.close();
		} catch (IOException e) {
			logger.error("araqne logdb: cannot close run", e);
		}

	}

	public void reset() throws IOException {
		if (run.indexFile != null)
			run.indexFile.share();

		if (run.dataFile != null)
			run.dataFile.share();

		loadCount = 0;
		prefetch = null;

		if (run.cached != null) {
			cachedIt = run.cached.iterator();
		} else {
			indexFile.seek(run.offset * LONG_SIZE);
			long dataOffset = indexFile.readLong();
			dataFile.seek(dataOffset);
		}
	}
}