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
package org.araqne.logdb.query.command;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.araqne.api.SystemProperty;
import org.araqne.logdb.FieldOrdering;
import org.araqne.logdb.ObjectComparator;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.ThreadSafe;
import org.araqne.logdb.VectorizedRowBatch;
import org.araqne.logdb.query.aggregator.AggregationField;
import org.araqne.logdb.query.aggregator.AggregationFunction;
import org.araqne.logdb.query.aggregator.VectorizedAggregationFunction;
import org.araqne.logdb.sort.CloseableIterator;
import org.araqne.logdb.sort.Item;
import org.araqne.logdb.sort.ItemChunk;
import org.araqne.logdb.sort.ParallelMergeSorter;
import org.araqne.logstorage.LogVector;
import org.araqne.logstorage.ObjectVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Stats extends QueryCommand implements FieldOrdering, ThreadSafe {
	private static final int OUTPUT_FLUSH_THRESHOLD = 2000;
	private static final int FLUSH_SIZE = 100000;
	private final Logger logger = LoggerFactory.getLogger(Stats.class);
	private final Logger compareLogger = LoggerFactory.getLogger("stats-key-compare");

	private final AggregationField[] fields;
	private final String[] clauses;
	private final int clauseCount;
	private final boolean useClause;
	private final KeyHolder EMPTY_KEY;
	private static final boolean discardNullGroup;

	// clone template
	private AggregationFunction[] funcs;
	private AggregationFunction[] emptyClauseFuncs;
	private List<String> fieldOrder;

	private ParallelMergeSorter sorter;
	private ConcurrentHashMap<KeyHolder, AggregationFunction[]> buffer;
	private AtomicLong inputCount = new AtomicLong();

	// output vectorization
	private int outputCount;
	private Object[][] keyVector;
	private Object[][] valVector;

	private FlushWorker flushWorker;

	static {
		discardNullGroup = SystemProperty.isEnabled("araqne.logdb.discard_null_group");
	}

	public Stats(AggregationField[] fields, String[] clause) {
		this.EMPTY_KEY = new KeyHolder(0);
		this.clauses = clause;
		this.clauseCount = clauses.length;
		this.useClause = clauseCount > 0;
		this.fields = fields;
		this.funcs = new AggregationFunction[fields.length];
		this.fieldOrder = new ArrayList<String>(Arrays.asList(clauses));

		// prepare template functions
		for (int i = 0; i < fields.length; i++) {
			AggregationField f = fields[i];
			this.funcs[i] = f.getFunction();
			this.fieldOrder.add(f.getName());
		}
	}

	@Override
	public String getName() {
		return "stats";
	}

	@Override
	public List<String> getFieldOrder() {
		return new ArrayList<String>(fieldOrder);
	}

	public List<AggregationField> getAggregationFields() {
		return Arrays.asList(fields);
	}

	public List<String> getClauses() {
		return Arrays.asList(clauses);
	}

	@Override
	public void onStart() {
		sorter = new ParallelMergeSorter(new ItemComparer(), FLUSH_SIZE);

		int queryId = 0;
		if (getQuery() != null)
			queryId = getQuery().getId();

		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd_HHmmss");
		sorter.setTag("_" + queryId + "_" + df.format(new Date()) + "_");

		this.buffer = new ConcurrentHashMap<KeyHolder, AggregationFunction[]>(FLUSH_SIZE);

		for (AggregationFunction f : funcs)
			f.clean();

		if (!useClause) {
			emptyClauseFuncs = new AggregationFunction[funcs.length];
			for (int i = 0; i < funcs.length; i++)
				emptyClauseFuncs[i] = funcs[i].clone();

			buffer.put(EMPTY_KEY, emptyClauseFuncs);
		}

		flushWorker = new FlushWorker();
		flushWorker.start();
	}

	@Override
	public void onPush(VectorizedRowBatch vbatch) {
		int newKeyCount = 0;
		KeyHolder keyHolder = EMPTY_KEY;
		Object[][] clauseValues = null;

		if (useClause) {
			keyHolder = new KeyHolder(clauseCount);

			clauseValues = new Object[clauseCount][];
			for (int i = 0; i < clauseCount; i++) {
				LogVector vec = vbatch.data.get(clauses[i]);
				if (vec != null)
					clauseValues[i] = vec.getArray();
				else
					clauseValues[i] = new Object[vbatch.size];
			}
		}

		Object[] keys = keyHolder.keys;

		flushWorker.rwLock.readLock().lock();
		try {
			if (vbatch.selectedInUse) {
				for (int i = 0; i < vbatch.size; i++) {
					int p = vbatch.selected[i];
					AggregationFunction[] fs = null;
					if (useClause) {
						for (int j = 0; j < clauseCount; j++)
							keys[j] = clauseValues[j][p];

						fs = buffer.get(keyHolder);
						if (fs == null) {
							fs = new AggregationFunction[funcs.length];
							for (int j = 0; j < fs.length; j++)
								fs[j] = funcs[j].clone();

							AggregationFunction[] oldFs = buffer.putIfAbsent(keyHolder.clone(), fs);
							if (oldFs != null)
								fs = oldFs;
							else
								newKeyCount++;
						}
					} else {
						fs = emptyClauseFuncs;
					}

					for (AggregationFunction f : fs) {
						if (f instanceof VectorizedAggregationFunction) {
							((VectorizedAggregationFunction) f).apply(vbatch, p);
						} else {
							f.apply(vbatch.row(p));
						}
					}
				}
			} else {
				for (int i = 0; i < vbatch.size; i++) {
					AggregationFunction[] fs = null;
					if (useClause) {
						for (int j = 0; j < clauseCount; j++)
							keys[j] = clauseValues[j][i];

						fs = buffer.get(keyHolder);
						if (fs == null) {
							fs = new AggregationFunction[funcs.length];
							for (int j = 0; j < fs.length; j++)
								fs[j] = funcs[j].clone();

							AggregationFunction[] oldFs = buffer.putIfAbsent(keyHolder.clone(), fs);
							if (oldFs != null)
								fs = oldFs;
							else
								newKeyCount++;
						}
					} else {
						fs = emptyClauseFuncs;
					}

					for (AggregationFunction f : fs) {
						if (f instanceof VectorizedAggregationFunction) {
							((VectorizedAggregationFunction) f).apply(vbatch, i);
						} else {
							f.apply(vbatch.row(i));
						}
					}
				}
			}
		} finally {
			flushWorker.rwLock.readLock().unlock();
		}

		flushWorker.countNewKey(newKeyCount);
		inputCount.addAndGet(vbatch.size);
	}

	@Override
	public void onPush(RowBatch rowBatch) {
		int newKeyCount = 0;
		KeyHolder keys = EMPTY_KEY;

		if (useClause)
			keys = new KeyHolder(clauseCount);

		inputCount.addAndGet(rowBatch.size);

		flushWorker.rwLock.readLock().lock();
		try {
			if (rowBatch.selectedInUse) {
				for (int index = 0; index < rowBatch.size; index++) {
					Row row = rowBatch.rows[rowBatch.selected[index]];
					if (useClause) {
						boolean isNullGroup = false;
						for (int i = 0; i < clauseCount; i++) {
							Object keyValue = row.get(clauses[i]);
							if (discardNullGroup && keyValue == null) {
								isNullGroup = true;
								break;
							}

							keys.keys[i] = keyValue;
						}

						if (isNullGroup)
							continue;
					}

					AggregationFunction[] fs = buffer.get(keys);
					if (fs == null) {
						fs = new AggregationFunction[funcs.length];
						for (int i = 0; i < fs.length; i++)
							fs[i] = funcs[i].clone();

						AggregationFunction[] oldFs = buffer.putIfAbsent(keys.clone(), fs);
						if (oldFs != null)
							fs = oldFs;
						else
							newKeyCount++;
					}

					for (AggregationFunction f : fs)
						f.apply(row);
				}
			} else {
				for (int i = 0; i < rowBatch.size; i++) {
					Row m = rowBatch.rows[i];
					if (useClause) {
						boolean isNullGroup = false;
						for (int d = 0; d < clauseCount; d++) {
							Object keyValue = m.get(clauses[d]);
							if (discardNullGroup && keyValue == null) {
								isNullGroup = true;
								break;
							}

							keys.keys[d] = keyValue;
						}

						if (isNullGroup)
							continue;
					}

					AggregationFunction[] fs = buffer.get(keys);
					if (fs == null) {
						fs = new AggregationFunction[funcs.length];
						for (int j = 0; j < fs.length; j++)
							fs[j] = funcs[j].clone();

						AggregationFunction[] oldFs = buffer.putIfAbsent(keys.clone(), fs);
						if (oldFs != null)
							fs = oldFs;
						else
							newKeyCount++;
					}

					for (AggregationFunction f : fs)
						f.apply(m);
				}
			}
		} finally {
			flushWorker.rwLock.readLock().unlock();
		}

		flushWorker.countNewKey(newKeyCount);
	}

	@Override
	public void onPush(Row m) {
		int newKeyCount = 0;
		flushWorker.rwLock.readLock().lock();
		try {
			KeyHolder keys = EMPTY_KEY;
			if (clauseCount > 0) {
				keys = new KeyHolder(clauseCount);

				for (int i = 0; i < clauseCount; i++) {
					Object keyValue = m.get(clauses[i]);
					if (discardNullGroup && keyValue == null)
						return;

					keys.keys[i] = keyValue;
				}
			}

			inputCount.incrementAndGet();

			AggregationFunction[] fs = buffer.get(keys);
			if (fs == null) {
				fs = new AggregationFunction[funcs.length];
				for (int i = 0; i < fs.length; i++)
					fs[i] = funcs[i].clone();

				AggregationFunction[] oldFs = buffer.putIfAbsent(keys, fs);
				if (oldFs != null)
					fs = oldFs;
				else
					newKeyCount++;
			}

			for (AggregationFunction f : fs)
				f.apply(m);
		} finally {
			flushWorker.rwLock.readLock().unlock();
		}

		flushWorker.countNewKey(newKeyCount);
	}

	@Override
	public boolean isReducer() {
		return true;
	}

	@Override
	public void onClose(QueryStopReason reason) {
		if (flushWorker != null)
			flushWorker.closed = true;

		// command is not started
		if (sorter == null)
			return;

		if (reason != QueryStopReason.End && reason != QueryStopReason.PartialFetch) {
			try {
				sorter.cancel();
				sorter = null;
			} catch (Throwable t) {
				logger.warn("araqne logdb: cannot close stats sorter, query [" + getQuery().getId() + ":"
						+ getQuery().getQueryString() + "]", t);
			}

			return;
		}

		logger.debug("araqne logdb: stats sort input count [{}]", inputCount);
		CloseableIterator it = null;
		try {
			// last flush
			flushWorker.join();

			// reclaim buffer (GC support)
			buffer = new ConcurrentHashMap<KeyHolder, AggregationFunction[]>();

			// sort
			it = sorter.sort();

			flushOutput();

			Object[] lastKeys = null;
			AggregationFunction[] fs = null;
			Item item = null;
			int count = 0;
			while (it.hasNext()) {
				item = (Item) it.next();
				count++;

				// first record or need to change merge set?
				if (lastKeys == null || !Arrays.equals(lastKeys, (Object[]) item.key)) {
					if (compareLogger.isDebugEnabled() && lastKeys != null)
						compareLogger.debug("araqne logdb: stats key compare [{}] != [{}]", lastKeys[0],
								((Object[]) item.key)[0]);

					// finalize last record (only if changing set)
					if (fs != null) {
						pass(fs, lastKeys);
					}

					// load new record
					fs = new AggregationFunction[funcs.length];
					int i = 0;
					Object[] rawFuncs = (Object[]) item.value;
					for (Object rawFunc : rawFuncs) {
						AggregationFunction f = funcs[i].clone();
						f.deserialize(rawFunc);
						fs[i++] = f;
					}
				} else {
					// merge
					int i = 0;
					for (AggregationFunction f : fs) {
						Object l = ((Object[]) item.value)[i];
						AggregationFunction f2 = funcs[i].clone();
						f2.deserialize(l);
						f.merge(f2);
						i++;
					}
				}

				lastKeys = (Object[]) item.key;
			}

			// write last merge set
			if (item != null) {
				pass(fs, lastKeys);
			}

			// write result for empty data set (only for no group clause)
			if (inputCount.get() == 0 && clauseCount == 0) {
				// write initial function values
				pass(funcs, null);
			}

			flushOutput();

			logger.debug("araqne logdb: sorted stats input [{}]", count);
		} catch (Throwable t) {
			getQuery().cancel(t);
			throw new IllegalStateException("sort failed, query " + query, t);
		} finally {
			if (it != null) {
				try {
					// close and delete final sorted run file
					it.close();
				} catch (IOException e) {
				}
			}

			// support sorter cache GC when query processing is ended
			sorter = null;
		}
	}

	private void pass(AggregationFunction[] fs, Object[] keys) {
		for (int i = 0; i < clauseCount; i++) {
			keyVector[i][outputCount] = keys[i];
		}

		for (int i = 0; i < funcs.length; i++) {
			valVector[i][outputCount] = fs[i].eval();
		}

		outputCount++;
		if (outputCount == OUTPUT_FLUSH_THRESHOLD)
			flushOutput();
	}

	private void flushOutput() {
		if (outputCount > 0) {
			Map<String, LogVector> m = new HashMap<String, LogVector>();
			VectorizedRowBatch vbatch = new VectorizedRowBatch();
			vbatch.size = outputCount;
			vbatch.data = m;

			for (int i = 0; i < clauseCount; i++)
				m.put(clauses[i], new ObjectVector(keyVector[i]));

			for (int i = 0; i < funcs.length; i++)
				m.put(fields[i].getName(), new ObjectVector(valVector[i]));

			pushPipe(vbatch);
		}

		outputCount = 0;
		keyVector = new Object[clauseCount][];
		for (int i = 0; i < clauseCount; i++)
			keyVector[i] = new Object[OUTPUT_FLUSH_THRESHOLD];

		valVector = new Object[funcs.length][];
		for (int i = 0; i < funcs.length; i++)
			valVector[i] = new Object[OUTPUT_FLUSH_THRESHOLD];
	}

	private static class ItemComparer implements Comparator<Item> {
		private ObjectComparator cmp = new ObjectComparator();

		@Override
		public int compare(Item o1, Item o2) {
			return cmp.compare(o1.key, o2.key);
		}
	}

	@Override
	public String toString() {
		String aggregation = "";
		int i = 0;
		for (AggregationField f : this.fields) {
			if (i++ != 0)
				aggregation += ",";
			aggregation += " " + f.toString();
		}

		String clause = "";
		if (clauseCount > 0) {
			clause = " by";
			i = 0;
			for (String c : clauses) {
				if (i++ != 0)
					clause += ",";
				clause += " " + c;
			}
		}

		return "stats" + aggregation + clause;
	}

	private static class KeyHolder {
		public final Object[] keys;

		public KeyHolder(int count) {
			this.keys = new Object[count];
		}

		@Override
		public int hashCode() {
			return Arrays.hashCode(keys);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			KeyHolder other = (KeyHolder) obj;
			return Arrays.equals(keys, other.keys);
		}

		public KeyHolder clone() {
			KeyHolder h = new KeyHolder(keys.length);
			for (int i = 0; i < keys.length; i++)
				h.keys[i] = keys[i];
			return h;
		}
	}

	private class FlushWorker extends Thread {
		private Semaphore semaphore = new Semaphore(FLUSH_SIZE, true);
		private ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
		private volatile boolean closed;

		public void countNewKey(int count) {
			do {
				try {
					semaphore.acquire(count);
					break;
				} catch (InterruptedException e) {
				}
			} while (!closed);
		}

		@Override
		public void run() {
			try {
				setName("Stats Flush Worker for query #" + getQuery().getId());
				while (!closed) {
					try {
						if (buffer.size() >= FLUSH_SIZE)
							flush();
						else
							LockSupport.parkNanos(1);
					} catch (IOException e) {
						logger.error("araqne logdb: query [" + query.getId() + "] stats flush failed", e);
					}
				}
			} finally {
				try {
					flush();
				} catch (IOException e) {
					logger.error("araqne logdb: query [" + query.getId() + "] stats flush failed", e);
				}

				logger.debug("araqne logdb: query [" + query.getId() + "] stats flush worker exit");
			}
		}

		private void flush() throws IOException {
			Map<KeyHolder, AggregationFunction[]> flushBuffer = null;
			try {
				rwLock.writeLock().lock();
				flushBuffer = buffer;
				buffer = new ConcurrentHashMap<KeyHolder, AggregationFunction[]>();
			} finally {
				rwLock.writeLock().unlock();
			}

			int keyCount = flushBuffer.size();
			if (logger.isDebugEnabled())
				logger.debug("araqne logdb: flushing stats buffer, [{}] keys", keyCount);

			if (keyCount > 0) {
				sorter.add(new ItemConverter(flushBuffer));
				semaphore.release(keyCount);
			}
		}
	}

	private static class ItemConverter implements ItemChunk {

		private Map<KeyHolder, AggregationFunction[]> map;

		public ItemConverter(Map<KeyHolder, AggregationFunction[]> map) {
			this.map = map;
		}

		@Override
		public Item[] getItems() {
			Item[] items = new Item[map.size()];
			int index = 0;
			for (Entry<KeyHolder, AggregationFunction[]> e : map.entrySet()) {
				KeyHolder keys = e.getKey();
				AggregationFunction[] fs = e.getValue();
				Object[] l = new Object[fs.length];
				int i = 0;
				for (AggregationFunction f : fs)
					l[i++] = f.serialize();

				items[index++] = new Item(keys.keys, l);
			}

			return items;
		}
	}
}
