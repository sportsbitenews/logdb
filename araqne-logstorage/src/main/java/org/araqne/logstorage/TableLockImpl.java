package org.araqne.logstorage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.araqne.logstorage.TableLock.Purpose;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableLockImpl {
	public static Logger logger = LoggerFactory.getLogger(TableLockImpl.class);
	static final int EXCLUSIVE = 65535;
	Semaphore sem = new Semaphore(EXCLUSIVE, true);
	
	public static UUID READ_LOCK_UUID = UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff");

	private static AtomicLong nextGuid = new AtomicLong(0);
	
	private static class PurposeImpl implements Purpose {
		String name;
		UUID uuid;
		int count;
		
		public PurposeImpl(String purpose) {
			name = purpose;
			uuid = UUID.randomUUID();
			count = 1;
		}
		
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((name == null) ? 0 : name.hashCode());
			result = prime * result + ((uuid == null) ? 0 : uuid.hashCode());
			return result;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			PurposeImpl other = (PurposeImpl) obj;
			if (name == null) {
				if (other.name != null)
					return false;
			} else if (!name.equals(other.name))
				return false;
			if (uuid == null) {
				if (other.uuid != null)
					return false;
			} else if (!uuid.equals(other.uuid))
				return false;
			return true;
		}
		
		@Override
		public String getName() {
			return name;
		}
		
		@Override
		public UUID getUUID() {
			return uuid;
		}
		
		@Override
		public String toString() {
			return String.format("%s:%d", name, count);
		}
	};

	String owner;
	Map<String, PurposeImpl> purposes;
	private int tid;

	@Override
	public String toString() {
		return String.format("TableLockImpl [owner=%s, purposes=%s]", owner, purposes);
	}

	public TableLockImpl(int tableId) {
		this.tid = tableId;
		owner = null;
		purposes = new HashMap<String, PurposeImpl>();
	}

	public int availableShared() {
		return sem.availablePermits();
	}

	public class ReadLock implements TableLock {
		long ownerTid = -1;

		@Override
		public UUID lock() {
			sem.acquireUninterruptibly();
			ownerTid = Thread.currentThread().getId();
			return READ_LOCK_UUID;
		}

		@Override
		public UUID lockInterruptibly() throws InterruptedException {
			sem.acquire();
			ownerTid = Thread.currentThread().getId();
			return READ_LOCK_UUID;
		}

		@Override
		public UUID tryLock() {
			boolean locked = sem.tryAcquire();
			if (locked) {
				ownerTid = Thread.currentThread().getId();
				return READ_LOCK_UUID;
			} else {
				return null;
			}
		}

		@Override
		public UUID tryLock(long time, TimeUnit unit) throws InterruptedException {
			boolean locked = sem.tryAcquire(time, unit);
			if (locked) {
				ownerTid = Thread.currentThread().getId();
				return READ_LOCK_UUID;
			} else {
				return null;
			}
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
		public int getTableId() {
			return tid;
		}

		@Override
		public String getLockOwner() {
			return TableLockImpl.this.getOwner();
		}

		@Override
		public Collection<Purpose> getPurposes() {
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
		public UUID lock() {
			assert acquierer != null;
			if (checkReentrant()) {
				return TableLockImpl.this.getUuid(purpose);
			}
			sem.acquireUninterruptibly(EXCLUSIVE);
			onLockAcquired();
			return TableLockImpl.this.getUuid(purpose);
		}

		private void onLockAcquired() {
			if (!acquierer.equals(TableLockImpl.this.owner)) {
				TableLockImpl.this.owner = acquierer;
				TableLockImpl.this.purposes.clear();
			}
			TableLockImpl.this.acquirePurpose(purpose);
		}

		@Override
		public UUID lockInterruptibly() throws InterruptedException {
			if (checkReentrant()) {
				return TableLockImpl.this.getUuid(purpose);
			}
			sem.acquire(EXCLUSIVE);
			onLockAcquired();
			return TableLockImpl.this.getUuid(purpose);
		}

		@Override
		public UUID tryLock() {
			if (checkReentrant()) {
				return TableLockImpl.this.getUuid(purpose);
			}
			boolean locked = sem.tryAcquire(EXCLUSIVE);
			if (locked) {
				onLockAcquired();
				return TableLockImpl.this.getUuid(purpose);
			} else {
				return null;
			}
		}

		@Override
		public UUID tryLock(long time, TimeUnit unit) throws InterruptedException {
			if (checkReentrant())
				return TableLockImpl.this.getUuid(purpose);
			boolean locked = sem.tryAcquire(EXCLUSIVE, time, unit);
			if (locked) {
				onLockAcquired();
				return TableLockImpl.this.getUuid(purpose);
			} else {
				return null;
			}
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
				releasePurpose(purpose);
				if (TableLockImpl.this.purposes.size() == 0) {
					TableLockImpl.this.owner = null;
					return true;
				}
				return false;
			}
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
		public Collection<Purpose> getPurposes() {
			return TableLockImpl.this.getPurposes();
		}
	}

	public TableLock writeLock(String owner, String purpose) {
		if (owner == null)
			throw new IllegalArgumentException("owner argument cannot be null");
		return new WriteLock(owner, purpose);
	}

	// being called with mutex
	public void acquirePurpose(String purpose) {
		if (purposes.containsKey(purpose)) {
			purposes.get(purpose).count++;
		} else {
			purposes.put(purpose, new PurposeImpl(purpose));
		}
	}

	// being called with mutex
	public void releasePurpose(String purpose) {
		if (purposes.containsKey(purpose)) {
			PurposeImpl p = purposes.get(purpose);
			p.count--;
			if (p.count == 0)
				purposes.remove(purpose);
		} else {
			// ignore 
			// throw new IllegalStateException("doesn't contain the purpose [" + purpose + "]");
		}
		
	}

	public UUID getUuid(String purpose) {
		PurposeImpl p = purposes.get(purpose);
		if (p != null)
			return p.uuid;
		else
			return null;
	}

	public String getOwner() {
		return owner;
	}

	public int getReentrantCount() {
		return purposes.size();
	}

	public Collection<Purpose> getPurposes() {
		ArrayList<Purpose> purposeList = new ArrayList<Purpose>();
		for (Entry<String, PurposeImpl> p: purposes.entrySet()) {
			purposeList.add(p.getValue());
		}
		return purposeList;
	}	
}
