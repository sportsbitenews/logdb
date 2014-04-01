package org.araqne.logstorage;

public class LockStatus {
	boolean locked;
	String owner;
	int availableShared;
	
	public LockStatus(String owner, int availableShared) {
		this.locked = true;
		this.owner = owner;
		this.availableShared = availableShared;
	}
	
	public LockStatus(int availableShared) {
		this.locked = false;
		this.owner = null;
		this.availableShared = availableShared;
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

}