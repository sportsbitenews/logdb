package org.araqne.logstorage.engine;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.araqne.logstorage.TableLock;

public class FallbackLock implements Lock {

	private TableLock iLock;

	public FallbackLock(TableLock managedLock) {
		this.iLock = managedLock;
	}

	public void lock() {
		while (true) {
			try {
				lockInterruptibly();
				return;
			} catch (InterruptedException e) {
			}
		}
	}

	public TableLock getInternal() {
		return iLock;
	}

	public void lockInterruptibly() throws InterruptedException {
		tryLock(Integer.MAX_VALUE, TimeUnit.SECONDS);
	}

	public boolean tryLock() {
		return iLock.tryLock();
	}

	private boolean callFallbacks(int tid) {
//		System.out.println("fallback checking");
		return false;
	}
	
	private static ThreadLocal<Random> rand = new ThreadLocal<Random>() {
		@Override
		protected Random initialValue() {
			return new Random();
		}
	};

	private static class BackOff {
		private int min;
		private int max;

		BackOff(long time, TimeUnit unit) {
			this.min = 0;
			this.max = (int) Math.min(unit.toNanos(time) / 100, Integer.MAX_VALUE / 100);
		}

		long nextBackOff() {
			int rmin = 1000;
			int rmax = Math.max(1001, min);
			int ni = rmin + rand.get().nextInt(rmax - rmin);
			min = Math.min(min == 0 ? 1 : min * 2, max);
			return ni;
		}
	}

	public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
		long to = unit.toNanos(time);
		BackOff bo = new BackOff(time, unit);
		boolean locked = iLock.tryLock();
		while (!locked && to > 0) {
			boolean handled = callFallbacks(iLock.getTableId());
			if (!handled) {
				long cbo = bo.nextBackOff();
				to -= cbo;
				locked = iLock.tryLock(cbo, TimeUnit.NANOSECONDS);
			}
		}
		return locked;
	}

	public void unlock() {
		iLock.unlock();
	}

	public Condition newCondition() {
		return iLock.newCondition();
	}
	
	private static class MockTableLock extends ReentrantLock implements TableLock {
		private static final long serialVersionUID = 1L;

		@Override
		public int getTableId() {
			return 0;
		}
	}

	public static void main(String[] args) throws InterruptedException {
		final FallbackLock l = new FallbackLock(new MockTableLock());
		Thread t1 = new Thread(new Runnable() {
			public void run() {
				try {
					l.lock();
					System.out.println("T1 locked");
					Thread.sleep(5000);
				} catch (InterruptedException e) {
				} finally {
					l.unlock();
					System.out.printf("%d: T1 unlocked%n", System.nanoTime());
				}
			}
		});
		t1.start();

		Thread.sleep(100);

		Thread t2 = new Thread(new Runnable() {
			public void run() {
				boolean locked = false;
				try {
					locked = l.tryLock(10, TimeUnit.SECONDS);
					if (locked)
						System.out.printf("%d: T2 locked%n", System.nanoTime());
					else
						System.out.printf("%d: T2 lock failed%n", System.nanoTime());
				} catch (InterruptedException e) {
					e.printStackTrace();
				} finally {
					if (locked) {
						l.unlock();
						System.out.println("T2 unlocked");
					}
				}
			}
		});
		t2.start();
	}
}