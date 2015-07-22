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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParallelMergeSorter {
	private static final int DEFAULT_CACHE_SIZE = 10000;
	private static final int DEFAULT_RUN_LENGTH = 50000;
	private final Logger logger = LoggerFactory.getLogger(ParallelMergeSorter.class);
	private final int runLength;
	private Queue<Run> runs = new LinkedBlockingDeque<Run>();
	private Queue<PartitionMergeTask> merges = new LinkedBlockingQueue<PartitionMergeTask>();
	private LinkedList<Item> buffer;
	private Comparator<Item> comparator;
	private AtomicInteger runIndexer;
	private volatile int flushTaskCount;
	private AtomicInteger cacheCount;
	private Object flushDoneSignal = new Object();
	private ExecutorService executor;
	private CountDownLatch mergeLatch;
	private volatile boolean canceled;
	private String tag = "";

	// end of input stream, do not allow add()
	private volatile boolean eos;

	public ParallelMergeSorter(Comparator<Item> comparator) {
		this(comparator, DEFAULT_RUN_LENGTH, DEFAULT_CACHE_SIZE);
	}

	public ParallelMergeSorter(Comparator<Item> comparator, int runLength) {
		this(comparator, runLength, DEFAULT_CACHE_SIZE);
	}

	public ParallelMergeSorter(Comparator<Item> comparator, int runLength, int memoryRunCount) {
		this.runLength = runLength;
		this.comparator = comparator;
		this.buffer = new LinkedList<Item>();
		this.runIndexer = new AtomicInteger();
		this.executor = new ThreadPoolExecutor(8, 8, 10, TimeUnit.SECONDS, new LimitedQueue<Runnable>(8), new CallerRunsPolicy());
		this.cacheCount = new AtomicInteger(memoryRunCount);
	}

	public void setTag(String tag) {
		if (tag == null)
			throw new IllegalArgumentException("sort tag should not be null");

		this.tag = tag;
	}

	public class LimitedQueue<E> extends ArrayBlockingQueue<E> {
		private static final long serialVersionUID = 1L;

		public LimitedQueue(int maxSize) {
			super(maxSize);
		}

		@Override
		public boolean offer(E e) {
			// turn offer() and add() into a blocking calls (unless interrupted)
			try {
				put(e);
				return true;
			} catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
			}
			return false;
		}
	}

	public void add(Item item) throws IOException {
		if (eos)
			throw new IllegalStateException("sort ended");

		buffer.add(item);
		if (buffer.size() >= runLength)
			flushRun();
	}

	public void addAll(List<? extends Item> items) throws IOException {
		if (eos)
			throw new IllegalStateException("sort ended");

		buffer.addAll(items);
		if (buffer.size() >= runLength)
			flushRun();
	}

	private void flushRun() throws IOException, FileNotFoundException {
		LinkedList<Item> buffered = buffer;
		if (buffered.isEmpty())
			return;

		buffer = new LinkedList<Item>();
		synchronized (flushDoneSignal) {
			flushTaskCount++;
		}
		executor.submit(new FlushWorker(buffered));
	}

	public CloseableIterator sort() throws IOException {
		eos = true;

		try {
			List<Partition> partitions = buildPartitions();

			// n-way merge
			CloseableIterator it = mergeAll(partitions);
			logger.trace("merge ended");
			return it;
		} catch (IOException e) {
			purgeAll();
			throw e;
		} catch (Throwable t) {
			purgeAll();
			String msg = "sort failed - " + t.toString();
			throw new IOException(msg, t);
		} finally {
			executor.shutdown();
		}
	}

	/**
	 * purge all runs if undesired exception occurs
	 */
	private void purgeAll() {
		for (Run run : runs) {
			if (run.indexFile != null)
				run.indexFile.delete();

			if (run.dataFile != null)
				run.dataFile.delete();
		}
	}

	public void cancel() throws IOException {
		eos = true;
		canceled = true;
		sort().close();
	}

	private List<Partition> buildPartitions() throws IOException {
		// flush rest objects
		if (!canceled)
			flushRun();

		buffer = null;
		logger.trace("flush finished.");

		// wait flush done
		while (true) {
			synchronized (flushDoneSignal) {
				if (flushTaskCount == 0)
					break;

				try {
					flushDoneSignal.wait();
				} catch (InterruptedException e) {
				}
				logger.debug("araqne logdb: remaining runs {}, task count: {}", runs.size(), flushTaskCount);
			}
		}

		// partition
		logger.trace("araqne logdb: start partitioning");
		long begin = new Date().getTime();
		Partitioner partitioner = new Partitioner(comparator);
		List<SortedRun> sortedRuns = new LinkedList<SortedRun>();
		for (Run run : runs)
			sortedRuns.add(new SortedRunImpl(run));

		try {
			int partitionCount = getProperPartitionCount();
			List<Partition> partitions = partitioner.partition(partitionCount, sortedRuns);
			
			// run should be purged at caller if partitioning is failed
			runs.clear();

			long elapsed = new Date().getTime() - begin;
			logger.trace("araqne logdb: [{}] partitioning completed in {}ms", partitionCount, elapsed);
			return partitions;

		} catch (RuntimeException e) {
			throw e;
		} finally {
			for (SortedRun r : sortedRuns) {
				try {
					((SortedRunImpl) r).close();
				} catch (Throwable t) {
					logger.debug("araqne logdb: cannot close sort run", t);
				}
			}
		}
	}

	private static int getProperPartitionCount() {
		int processors = Runtime.getRuntime().availableProcessors();
		int count = 2;
		while (count < processors)
			count <<= 1;

		return count;
	}

	private static class SortedRunImpl implements SortedRun {
		private RunInputRandomAccess ra;
		private Run run;

		public SortedRunImpl(Run run) throws IOException {
			this.run = run;
		}

		@Override
		public int length() {
			return run.length;
		}

		@Override
		public Item get(int offset) {
			try {
				return ra.get(offset);
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		}

		public void close() {
			if (ra != null)
				ra.close();
			ra = null;
		}

		@Override
		public void open() throws IOException {
			this.ra = new RunInputRandomAccess(run);
		}
	}

	private CloseableIterator mergeAll(List<Partition> partitions) throws IOException {
		// enqueue partition merge
		int id = 0;

		List<PartitionMergeTask> tasks = new ArrayList<PartitionMergeTask>();
		for (Partition p : partitions) {
			List<Run> runParts = new LinkedList<Run>();
			for (SortedRunRange range : p.getRunRanges()) {
				SortedRunImpl ri = (SortedRunImpl) range.getRun();
				Run run = ri.run;
				int newId = runIndexer.incrementAndGet();

				if (run.cached != null) {
					List<Item> sublist = run.cached.subList(range.getFrom(), range.getTo() + 1);
					Run r = new Run(newId, sublist);
					runParts.add(r);
				} else {
					Run r = new Run(newId, range.length(), run.indexFile.share(), run.dataFile.share(), range.getFrom());
					runParts.add(r);
				}
			}

			if (runParts.size() > 0) {
				PartitionMergeTask task = new PartitionMergeTask(id++, runParts);
				tasks.add(task);
			}
		}

		mergeLatch = new CountDownLatch(tasks.size());
		for (PartitionMergeTask task : tasks) {
			merges.add(task);
			executor.submit(new MergeWorker(task));
		}

		// wait partition merge
		while (mergeLatch.getCount() > 0) {
			try {
				mergeLatch.await();
			} catch (InterruptedException e) {
				logger.debug("araqne logdb: merge latch interrupted at count [{}]", mergeLatch.getCount());
			}
		}

		// final merge
		ArrayList<PartitionMergeTask> l = new ArrayList<PartitionMergeTask>();
		while (true) {
			PartitionMergeTask t = merges.poll();
			if (t == null)
				break;
			l.add(t);
		}

		Collections.sort(l);

		ArrayList<Run> finalRuns = new ArrayList<Run>();
		for (PartitionMergeTask t : l) {
			finalRuns.add(t.output);
		}

		return concat(finalRuns);
	}

	private class FlushWorker implements Runnable {
		private LinkedList<Item> buffered;

		public FlushWorker(LinkedList<Item> list) {
			buffered = list;
		}

		@Override
		public void run() {
			try {
				doFlush();
			} catch (Throwable t) {
				logger.error("araqne logdb: failed to flush", t);
			} finally {
				synchronized (flushDoneSignal) {
					flushTaskCount--;
					flushDoneSignal.notifyAll();
				}
			}
		}

		private void doFlush() throws IOException {
			if (canceled)
				return;

			Collections.sort(buffered, comparator);

			int id = runIndexer.incrementAndGet();
			RunOutput out = new RunOutput(id, buffered.size(), cacheCount, tag);
			try {
				for (Item o : buffered)
					out.write(o);
			} finally {
				Run run = out.finish();
				runs.add(run);
			}
		}
	}

	private class MergeWorker implements Runnable {
		private PartitionMergeTask task;

		public MergeWorker(PartitionMergeTask task) {
			this.task = task;
		}

		@Override
		public void run() {
			try {
				Run merged = mergeRuns(task.runs);
				logger.debug("araqne logdb: merged run {}, input runs: {}", merged, task.runs);

				task.output = merged;
			} catch (Throwable t) {
				logger.error("araqne logdb: failed to merge " + task.runs, t);
			} finally {
				mergeLatch.countDown();
			}
		}

		private Run mergeRuns(List<Run> runs) throws IOException {
			if (runs.size() == 1)
				return runs.get(0);

			List<Run> phase = new ArrayList<Run>();
			for (int i = 0; i < 8; i++) {
				if (!runs.isEmpty())
					phase.add(runs.remove(0));
			}

			runs.add(merge(phase));
			return mergeRuns(runs);
		}

	}

	private CloseableIterator concat(List<Run> finalRuns) throws IOException {
		// empty iterator for no item scenario
		if (finalRuns.size() == 0)
			return new CacheRunIterator(new ArrayList<Item>().iterator());

		List<RunInput> iters = new ArrayList<RunInput>();
		for (Run run : finalRuns)
			iters.add(new RunInput(run, cacheCount));

		return new MultiRunIterator(iters);
	}

	private Run merge(List<Run> runs) throws IOException {
		if (runs.size() == 0)
			throw new IllegalArgumentException("runs should not be empty");

		if (runs.size() == 1) {
			return runs.get(0);
		}

		logger.debug("araqne logdb: begin {}way merge, {}", runs.size(), runs);
		ArrayList<RunInput> inputs = new ArrayList<RunInput>();
		PriorityQueue<RunItem> q = new PriorityQueue<RunItem>(runs.size(), new RunItemComparater());
		RunOutput r3 = null;
		try {
			int total = 0;
			for (Run r : runs) {
				inputs.add(new RunInput(r, cacheCount));
				total += r.length;
			}

			int id = runIndexer.incrementAndGet();
			r3 = new RunOutput(id, total, cacheCount, true, tag);

			while (!canceled) {
				// load next inputs
				for (RunInput input : inputs) {
					if (input.loaded == null && input.hasNext()) {
						input.loaded = input.next();
						q.add(new RunItem(input, input.loaded));
					}
				}

				RunItem item = q.poll();
				if (item == null)
					break;

				r3.write(item.item);
				item.runInput.loaded = null;
			}
		} finally {
			for (RunInput input : inputs)
				input.purge();

			if (r3 != null)
				return r3.finish();
		}

		logger.error("araqne logdb: merge cannot reach here, bug check!");
		return null;
	}

	private class RunItemComparater implements Comparator<RunItem> {

		@Override
		public int compare(RunItem o1, RunItem o2) {
			return comparator.compare(o1.item, o2.item);
		}

	}

	private static class RunItem {
		private RunInput runInput;
		private Item item;

		public RunItem(RunInput runInput, Item item) {
			this.runInput = runInput;
			this.item = item;
		}
	}

	private class PartitionMergeTask implements Comparable<PartitionMergeTask> {
		private int id;
		private List<Run> runs;
		private Run output;

		public PartitionMergeTask(int id, List<Run> runs) {
			this.id = id;
			this.runs = runs;
		}

		@Override
		public int compareTo(PartitionMergeTask o) {
			return id - o.id;
		}
	}
}
