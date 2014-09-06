package org.araqne.logstorage;

import java.util.Collection;

public class LockStatus {
	boolean locked;
	String owner;
	int availableShared;
	int reentrantCount;
	Collection<String> purposes;
	
	public LockStatus(String owner, int availableShared, int reentrantCount, Collection<String> purposes) {
		this.locked = true;
		this.owner = owner;
		this.availableShared = availableShared;
		this.reentrantCount = reentrantCount;
		this.purposes = purposes;
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

	public Collection<String> getPurposes() {
		return purposes;
	}

}