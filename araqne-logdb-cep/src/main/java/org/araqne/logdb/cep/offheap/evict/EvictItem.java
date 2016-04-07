package org.araqne.logdb.cep.offheap.evict;

public class EvictItem implements Comparable<EvictItem> {

	private long time;
	private long address;

	public EvictItem(long time, long address) {
		this.time = time;
		this.address = address;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public long getAddress() {
		return address;
	}

	public void setAddress(long address) {
		this.address = address;
	}

	@Override
	public String toString() {
		return "time = " + time + ", address = " + address;
	}

	@Override
	public int compareTo(EvictItem o) {
		if (o == null)
			return -1;

		return (time < o.getTime()) ? -1 : ((time == o.getTime()) ? 0 : 1);
	}

}
