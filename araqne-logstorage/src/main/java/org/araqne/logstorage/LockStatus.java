package org.araqne.logstorage;

public class LockStatus {
	boolean locked;
	String owner;
	int availableShared;
	int reentrantCount;
	
	public LockStatus(String owner, int availableShared, int reentrantCount) {
		this.locked = true;
		this.owner = owner;
		this.availableShared = availableShared;
		this.reentrantCount = reentrantCount;
	}
	
	public LockStatus(int availabledShared) {
		this.locked = false;
		this.owner = null;
		this.availableShared = availabledShared;
		this.reentrantCount = 0;
	}
	
	public boolean isLocked() {
		return locked;
	}

	public Object getOwner() {
		return owner;
	}
	
	public int getAvailableShared() {
		return availableShared;
	}
	
	public int getReentrantCount() {
		return reentrantCount;
	}

}