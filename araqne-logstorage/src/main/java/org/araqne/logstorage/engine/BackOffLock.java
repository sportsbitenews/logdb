package org.araqne.logstorage.engine;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.araqne.logstorage.TableLock;

public class BackOffLock {
	private static ThreadLocal<Random> rand = new ThreadLocal<Random>() {
		@Override
		protected Random initialValue() {
			return new Random();
		}
	};

	private long min;
	private long max;
	private static final long MAX_WAIT = Integer.MAX_VALUE / 10;

	private long to;
	private boolean locked = false;

	private TableLock lock;

	private int tryCnt = 0;

	public BackOffLock(TableLock l) {
		this.lock = l;
		to = Integer.MIN_VALUE;
		this.min = 10000; // 0.01ms
		this.max = MAX_WAIT;
	}

	public BackOffLock(TableLock l, long time, TimeUnit unit) {
		this.lock = l;
		to = unit.toNanos(time);

		this.min = 10000; // 0.01ms
		this.max = (int) Math.min(unit.toNanos(time) / 100, MAX_WAIT);
	}

	private long nextBackOff() {
		long rmin = min;
		long rmax = rmin + Math.max(100000, min); // 0.1ms
		long ni = rmin + rand.get().nextInt((int) (rmax - rmin));
		min = Math.min(min == 0 ? 1 : min * 2, max);
		return ni;
	}

	public boolean tryLock() throws InterruptedException {
		if (tryCnt++ != 0) {
			long cbo = nextBackOff();
			if (to != Integer.MIN_VALUE)
				to -= cbo;
			return (locked = lock.tryLock(cbo, TimeUnit.NANOSECONDS) != null);
		} else {
			return (locked = lock.tryLock() != null);
		}
	}

	public void setDone() {
		to = -1;
	}

	public boolean isDone() {
		return locked || (to != Integer.MIN_VALUE && to < 0);
	}

	public boolean hasLocked() {
		return locked;
	}

	public void unlock() {
		if (locked)
			lock.unlock();
	}

	public static void main(String[] args) throws InterruptedException {
		final TableLock l = new TableLock() {

			@Override
			public UUID lock() {
				l.lock();
				return uuid = UUID.randomUUID();
			}

			@Override
			public UUID lockInterruptibly() throws InterruptedException {
				l.lockInterruptibly();
				return uuid = UUID.randomUUID();
			}

			@Override
			public UUID tryLock() {
				if (l.tryLock()) {
					return uuid = UUID.randomUUID();
				} else {
					return null;
				}
			}

			@Override
			public UUID tryLock(long time, TimeUnit unit) throws InterruptedException {
				if (l.tryLock(time, unit)) {
					return uuid = UUID.randomUUID();
				} else {
					return null;
				}
			}

			@Override
			public void unlock() {
				uuid = null;
				l.unlock();
			}

			@Override
			public int getTableId() {
				return 0;
			}

			@Override
			public String getLockOwner() {
				return null;
			}

			@Override
			public Collection<Purpose> getPurposes() {
				return null;
			}
			Lock l = new ReentrantLock(true);
			UUID uuid = null;
		};
		l.lock();
		Thread t = new Thread(new Runnable() {

			@Override
			public void run() {
				BackOffLock bol = new BackOffLock(l);
				do {
					boolean locked;
					try {
						locked = bol.tryLock();
						if (locked) {
							System.out.println("locked");
						} else {
							System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()));
						}
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} while (!bol.isDone());
			}

		});

		t.start();
		t.join();
	}

}