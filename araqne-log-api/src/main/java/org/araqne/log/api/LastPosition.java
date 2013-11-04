package org.araqne.log.api;

import java.util.Date;

public class LastPosition {
	private int version;
	private String path;
	private long position;
	private Date lastSeen;

	public LastPosition(String path) {
		this.path = path;
	}

	public LastPosition(String path, long position, Date checkDate) {
		this.path = path;
		this.position = position;
		this.lastSeen = checkDate;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public long getPosition() {
		return position;
	}

	public void setPosition(long position) {
		this.position = position;
	}

	public Date getLastSeen() {
		return lastSeen;
	}

	public void setLastSeen(Date lastSeen) {
		this.lastSeen = lastSeen;
	}
}
