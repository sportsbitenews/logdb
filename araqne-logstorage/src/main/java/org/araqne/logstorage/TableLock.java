package org.araqne.logstorage;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class TableLock {
	final int EXCLUSIVE = 65535;
	Semaphore sem = new Semaphore(EXCLUSIVE, true);
	String owner;
	protected int holdCount = 0;

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
		if (owner == null)
			throw new IllegalArgumentException("owner argument cannot be null");
		return new Lock() {
			@Override
			public void lock() {
				try {
					synchronized(TableLock.this) {
						if (owner.equals(TableLock.this.owner)) {
							TableLock.this.holdCount += 1;
							return;
						}
					}
					sem.acquire(EXCLUSIVE);
					try {
						synchronized (TableLock.this) {
							if (owner.equals(TableLock.this.owner)) {
								TableLock.this.holdCount += 1;
							} else {
								TableLock.this.owner = owner;
								TableLock.this.holdCount = 1;
							}
						}
					} catch (RuntimeException t) {
						sem.release(EXCLUSIVE);
						throw t;
					}
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public void lockInterruptibly() throws InterruptedException {
				synchronized(TableLock.this) {
					if (owner.equals(TableLock.this.owner)) {
						TableLock.this.holdCount += 1;
						return;
					}
				}
				sem.acquire(EXCLUSIVE);
				try {
					synchronized (TableLock.this) {
						if (owner.equals(TableLock.this.owner)) {
							TableLock.this.holdCount += 1;
						} else {
							TableLock.this.owner = owner;
							TableLock.this.holdCount = 1;
						}
					}
				} catch (RuntimeException t) {
					sem.release(EXCLUSIVE);
					throw t;
				}
			}

			@Override
			public boolean tryLock() {
				synchronized(TableLock.this) {
					if (owner.equals(TableLock.this.owner)) {
						TableLock.this.holdCount += 1;
						return true;
					}
				}
				boolean locked = sem.tryAcquire(EXCLUSIVE);
				if (locked) {
					try {
						synchronized (TableLock.this) {
							if (owner.equals(TableLock.this.owner)) {
								TableLock.this.holdCount += 1;
							} else {
								TableLock.this.owner = owner;
								TableLock.this.holdCount = 1;
							}
						}
					} catch (RuntimeException t) {
						sem.release(EXCLUSIVE);
						throw t;
					}
				}
				return locked;
			}

			@Override
			public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
				synchronized(TableLock.this) {
					if (owner.equals(TableLock.this.owner)) {
						TableLock.this.holdCount += 1;
						return true;
					}
				}

				boolean locked = sem.tryAcquire(EXCLUSIVE, time, unit);
				if (locked) {
					try {
						synchronized (TableLock.this) {
							if (owner.equals(TableLock.this.owner)) {
								TableLock.this.holdCount += 1;
							} else {
								TableLock.this.owner = owner;
								TableLock.this.holdCount = 1;
							}
						}
					} catch (RuntimeException t) {
						sem.release(EXCLUSIVE);
						throw t;
					}
				} 
				return locked;
			}

			@Override
			public void unlock() {
				if (TableLock.this.owner == null) {
					return;
				} else {
					synchronized(TableLock.this) {
						if (owner.equals(TableLock.this.owner)) {
							TableLock.this.holdCount -= 1;
							if (TableLock.this.holdCount == 0) {
								TableLock.this.owner = null;
								sem.release(EXCLUSIVE);
							}
						} else {
							throw new IllegalMonitorStateException(owner + " cannot unlock this lock now: " + TableLock.this.owner);
						}
						
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
