package org.araqne.logstorage;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableLockImpl {
	public static Logger logger = LoggerFactory.getLogger(TableLockImpl.class);
	static final int EXCLUSIVE = 65535;
	Semaphore sem = new Semaphore(EXCLUSIVE, true);

	String owner;
	List<String> purposes;
	private int tid;

	@Override
	public String toString() {
		return String.format("TableLockImpl [owner=%s, purposes=%s]", owner, purposes);
	}

	public TableLockImpl(int tableId) {
		this.tid = tableId;
		owner = null;
		purposes = new LinkedList<String>();
	}

	public int availableShared() {
		return sem.availablePermits();
	}

	public class ReadLock implements TableLock {
		long ownerTid = -1;

		@Override
		public void lock() {
			sem.acquireUninterruptibly();
			ownerTid = Thread.currentThread().getId();
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

		@Override
		public int getTableId() {
			return tid;
		}

		@Override
		public String getLockOwner() {
			return TableLockImpl.this.getOwner();
		}

		@Override
		public Collection<String> getPurposes() {
			return TableLockImpl.this.getPurposes();
		}
	}

	public TableLock readLock() {
		return new ReadLock();
	}

	public class WriteLock implements TableLock {
		final public String acquierer;
		final public String purpose;

		public WriteLock(String owner, String purpose) {
			this.acquierer = owner;
			this.purpose = purpose;
		}

		@Override
		public void lock() {
			assert acquierer != null;
			if (checkReentrant()) {
				return;
			}
			sem.acquireUninterruptibly(EXCLUSIVE);
			onLockAcquired();
		}

		private void onLockAcquired() {
			if (!acquierer.equals(TableLockImpl.this.owner)) {
				TableLockImpl.this.owner = acquierer;
				TableLockImpl.this.purposes.clear();
			}
			TableLockImpl.this.purposes.add(purpose);
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
			synchronized (TableLockImpl.this) {
				if (acquierer.equals(TableLockImpl.this.owner)) {
					onLockAcquired();
					return true;
				}
				return false;
			}
		}

		@Override
		public void unlock() {
			if (TableLockImpl.this.owner == null) {
				return;
			} else {
				if (onLockReleased())
					sem.release(EXCLUSIVE);
			}
		}

		private boolean onLockReleased() {
			synchronized (TableLockImpl.this) {
				if (!acquierer.equals(TableLockImpl.this.owner)) {
					throw new IllegalMonitorStateException(owner + " cannot unlock this lock now: "
							+ TableLockImpl.this.owner);
				}
				TableLockImpl.this.purposes.remove(purpose);
				if (TableLockImpl.this.purposes.size() == 0) {
					TableLockImpl.this.owner = null;
					return true;
				}
				return false;
			}
		}

		@Override
		public Condition newCondition() {
			throw new UnsupportedOperationException();
		}

		@Override
		public int getTableId() {
			return tid;
		}
		
		@Override
		public String getLockOwner() {
			return TableLockImpl.this.getOwner();
		}

		@Override
		public Collection<String> getPurposes() {
			return TableLockImpl.this.getPurposes();
		}

	}

	public TableLock writeLock(String owner, String purpose) {
		if (owner == null)
			throw new IllegalArgumentException("owner argument cannot be null");
		return new WriteLock(owner, purpose);
	}

	public String getOwner() {
		return owner;
	}

	public int getReentrantCount() {
		return purposes.size();
	}

	public Collection<String> getPurposes() {
		return Collections.unmodifiableCollection(purposes);
	}

}
