package org.araqne.logstorage;

import java.util.ArrayList;
import java.util.Collection;

import org.araqne.logstorage.TableLock.Purpose;

public class LockStatus {
	boolean locked;
	String owner;
	int availableShared;
	int reentrantCount;
	Collection<Purpose> purposes;
	
	public LockStatus(String owner, int availableShared, int reentrantCount, Collection<Purpose> purposes) {
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
	
	@Override
	public String toString() {
		return "LockStatus [locked=" + locked + ", owner=" + owner + ", availableShared="
				+ availableShared + ", reentrantCount=" + reentrantCount + ", purposes=" + purposes
				+ "]";
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
		ArrayList<String> purposeList = new ArrayList<String>();
		
		if (purposes == null)
			return purposeList;
		
		for (Purpose p: purposes) {
			purposeList.add(p.toString());
		}
		return purposeList;
	}

}