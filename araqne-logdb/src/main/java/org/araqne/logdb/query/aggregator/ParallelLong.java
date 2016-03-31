package org.araqne.logdb.query.aggregator;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParallelLong {
	private static final int MAX_SLOT_COUNT = 16;
	static final int SLOT_COUNT;
	static final Random rng = new Random();
	final AtomicLong[] cells;

	static {
		int d = Runtime.getRuntime().availableProcessors();
		if (d >= MAX_SLOT_COUNT)
			d = 16;
		
		// override default slot count
		String s = System.getProperty("araqne.logdb.counter_slot");
		if (s != null) {
			try {
				d = Integer.parseInt(s);
				Logger slog = LoggerFactory.getLogger(ParallelLong.class);
				slog.info("araqne logdb: use [{}] slot for parallel long", d);
			} catch (NumberFormatException e) {
			}
		}

		SLOT_COUNT = d;
	}

	private ThreadLocal<Integer> slot = new ThreadLocal<Integer>() {
		@Override
		protected Integer initialValue() {
			return rng.nextInt(SLOT_COUNT);
		}
	};

	public ParallelLong(long initialValue) {
		cells = new AtomicLong[SLOT_COUNT];
		for (int i = 0; i < cells.length; i++)
			cells[i] = new AtomicLong(0);
	}

	public long get() {
		long sum = 0;
		for (int i = 0; i < SLOT_COUNT; i++)
			sum += cells[i].get();
		return sum;
	}

	public void set(long value) {
		for (int i = 0; i < SLOT_COUNT; i++)
			cells[i].set(0);

		cells[0].set(value);
	}

	// no return, just for compile
	public void incrementAndGet() {
		cells[slot.get()].incrementAndGet();
	}

	// no return, just for compile
	public void addAndGet(long value) {
		cells[slot.get()].addAndGet(value);
	}
}
