package org.araqne.logdb.query.command;

import java.util.concurrent.atomic.AtomicLong;

import org.araqne.cron.AbstractTickTimer;
import org.araqne.cron.TickService;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.ThreadSafe;

public class RateLimit extends QueryCommand implements ThreadSafe {

	private TickService tickService;
	private CounterReset reset = new CounterReset();
	private AtomicLong counter = new AtomicLong();
	private int limit;

	public RateLimit(TickService tickService, int limit) {
		this.tickService = tickService;
		this.limit = limit;
	}

	@Override
	public String getName() {
		return "ratelimit";
	}

	@Override
	public void onPush(Row row) {
		long count = counter.incrementAndGet();
		if (count >= limit) {
			synchronized (counter) {
				try {
					while (counter.get() >= limit)
						counter.wait(10);
				} catch (InterruptedException e) {
				}
			}
		}

		pushPipe(row);
	}

	@Override
	public void onPush(RowBatch rowBatch) {
		long count = counter.addAndGet(rowBatch.size);
		if (count >= limit) {
			synchronized (counter) {
				try {
					while (counter.get() >= limit)
						counter.wait(10);
				} catch (InterruptedException e) {
				}
			}
		}

		pushPipe(rowBatch);
	}

	@Override
	public void onStart() {
		tickService.addTimer(reset);
	}

	@Override
	public void onClose(QueryStopReason reason) {
		tickService.removeTimer(reset);
	}

	@Override
	public String toString() {
		return "ratelimit " + limit;
	}

	private class CounterReset extends AbstractTickTimer {
		@Override
		public int getInterval() {
			return 1000;
		}

		@Override
		public void onTick() {
			counter.set(0);
		}
	}
}
