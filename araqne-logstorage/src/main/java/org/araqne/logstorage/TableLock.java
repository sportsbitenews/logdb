package org.araqne.logstorage;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableLock {
	public static Logger logger = LoggerFactory.getLogger(TableLock.class);
	static final int EXCLUSIVE = 65535;
	Semaphore sem = new Semaphore(EXCLUSIVE, true);

	String owner;
	int holdCount;
	List<String> purposes;

	@Override
	public String toString() {
		return String.format("TableLock [owner=%s, holdCount=%s, purposes=%s]", owner, holdCount, purposes);
	}

	public TableLock() {
		owner = null;
		holdCount = 0;
		purposes = new LinkedList<String>();
	}

	public int availableShared() {
		return sem.availablePermits();
	}

	public class ReadLock implements Lock {
		long ownerTid = -1;

		@Override
		public void lock() {
			try {
				sem.acquire();
				ownerTid = Thread.currentThread().getId();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void lockInterruptibly() throws InterruptedException {
			sem.acquire();
			ownerTid = Thread.currentThread().getId();
		}

		@Override
		public boolean tryLock() {
			boolean locked = sem.tryAcquire();
			if (locked) {
				ownerTid = Thread.currentThread().getId();
			}
			return locked;
		}

		@Override
		public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
			boolean locked = sem.tryAcquire(time, unit);
			if (locked) {
				ownerTid = Thread.currentThread().getId();
			}
			return locked;
		}

		@Override
		public void unlock() {
			if (ownerTid == -1)
				return;
			else if (ownerTid != Thread.currentThread().getId())
				throw new IllegalThreadStateException("unlocking from the thread which doesn't own the lock");

			ownerTid = -1;
			sem.release();
		}

		@Override
		public Condition newCondition() {
			throw new UnsupportedOperationException();
		}
	}

	public Lock readLock() {
		return new ReadLock();
	}

	public class WriteLock implements Lock {
		final public String acquierer;
		final public String purpose;

		public WriteLock(String owner, String purpose) {
			this.acquierer = owner;
			this.purpose = purpose;
		}

		@Override
		public void lock() {
			try {
				assert acquierer != null;
				if (checkReentrant()) {
					return;
				}
				sem.acquire(EXCLUSIVE);
				onLockAcquired();
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}

		private void onLockAcquired() {
			if (!acquierer.equals(TableLock.this.owner)) {
				TableLock.this.owner = acquierer;
				TableLock.this.purposes.clear();
			}
			TableLock.this.holdCount += 1;
			TableLock.this.purposes.add(purpose);
		}

		@Override
		public void lockInterruptibly() throws InterruptedException {
			if (checkReentrant()) {
				return;
			}
			sem.acquire(EXCLUSIVE);
			onLockAcquired();
		}

		@Override
		public boolean tryLock() {
			if (checkReentrant()) {
				return true;
			}
			boolean locked = sem.tryAcquire(EXCLUSIVE);
			if (locked) {
				onLockAcquired();
			}
			return locked;
		}

		@Override
		public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
			if (checkReentrant())
				return true;
			boolean locked = sem.tryAcquire(EXCLUSIVE, time, unit);
			if (locked) {
				onLockAcquired();
			}
			return locked;
		}

		private boolean checkReentrant() {
			synchronized (TableLock.this) {
				if (acquierer.equals(TableLock.this.owner)) {
					onLockAcquired();
					return true;
				}
				return false;
			}
		}

		@Override
		public void unlock() {
			if (TableLock.this.owner == null) {
				return;
			} else {
				if (onLockReleased())
					sem.release(EXCLUSIVE);
			}
		}

		private boolean onLockReleased() {
			synchronized (TableLock.this) {
				if (!acquierer.equals(TableLock.this.owner)) {
					throw new IllegalMonitorStateException(owner + " cannot unlock this lock now: "
							+ TableLock.this.owner);
				}
				TableLock.this.holdCount -= 1;
				TableLock.this.purposes.remove(purpose);
				if (TableLock.this.holdCount == 0) {
					if (!TableLock.this.purposes.isEmpty())
						logger.warn(
								"purposes isn't managed correctly: {}: {}", TableLock.this, TableLock.this.purposes);
					TableLock.this.owner = null;
					TableLock.this.purposes.clear();
					return true;
				}
				return false;
			}
		}

		@Override
		public Condition newCondition() {
			throw new UnsupportedOperationException();
		}
	}

	public Lock writeLock(String owner, String purpose) {
		if (owner == null)
			throw new IllegalArgumentException("owner argument cannot be null");
		return new WriteLock(owner, purpose);
	}

	public String getOwner() {
		return owner;
	}

	public int getReentrantCount() {
		return holdCount;
	}

	public Collection<String> getPurposes() {
		return Collections.unmodifiableCollection(purposes);
	}

}
