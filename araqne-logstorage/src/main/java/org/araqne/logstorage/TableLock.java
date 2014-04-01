package org.araqne.logstorage;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class TableLock {
	final int EXCLUSIVE = 65535;
	Semaphore sem = new Semaphore(EXCLUSIVE, true);
	String owner;
		
	public void unlockForced() {
		sem.release(EXCLUSIVE);
	}
	
	public int availableShared() {
		return sem.availablePermits();
	}
	
	public Lock readLock() {
		return new Lock() {
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

		};
	}

	public Lock writeLock(final String owner) {
		return new Lock() {
			@Override
			public void lock() {
				try {
					sem.acquire(EXCLUSIVE);
					TableLock.this.owner = owner;
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public void lockInterruptibly() throws InterruptedException {
				sem.acquire(EXCLUSIVE);
				TableLock.this.owner = owner;
			}

			@Override
			public boolean tryLock() {
				boolean locked = sem.tryAcquire(EXCLUSIVE);
				if (locked) {
					TableLock.this.owner = owner;
				}
				return locked;
			}

			@Override
			public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
				boolean locked = sem.tryAcquire(EXCLUSIVE, time, unit);
				if (locked) {
					TableLock.this.owner = owner;
				}
				return locked;
			}

			@Override
			public void unlock() {
				if (TableLock.this.owner == null) {
					return;
				} else {
					if (TableLock.this.owner.equals(owner)) {
						TableLock.this.owner = null;
						sem.release(EXCLUSIVE);
					} else {
						throw new IllegalMonitorStateException(owner + " cannot unlock this lock now: " + TableLock.this.owner);
					}
				}
			}

			@Override
			public Condition newCondition() {
				throw new UnsupportedOperationException();
			}

		};
	}

	public String getOwner() {
		return owner;
	}

}
