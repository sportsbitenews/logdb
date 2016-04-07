package org.araqne.logdb.cep.offheap.storageArea;

import org.araqne.logdb.cep.offheap.allocator.AllocableArea;

@SuppressWarnings("restriction")
public class UnsafeStringStorageArea extends AbsractUnsafeStorageArea<String> implements AllocableArea<String> {

	public UnsafeStringStorageArea() {
		super(1);
	}

	public UnsafeStringStorageArea(int initCapaciry) {
		super(1, initCapaciry);
	}

	@Override
	public String getValue(int index) {
		int valueLength = storage.getInt(index(index));
		byte[] ret = new byte[valueLength];
		for (int i = 0; i < valueLength; i++) {
			ret[i] = storage.getByte(index(index) + 4 + i);
		}
		return new String(ret);
	}

	@Override
	public void setValue(int index, String value) {
		byte[] bytes = value.getBytes();
		storage.putInt(index(index), bytes.length);
		for (int i = 0; i < bytes.length; i++) {
			storage.putByte(index(index) + 4 + i, bytes[i]);
		}
	}

	@Override
	public void setAddress(int index, int address) {
		storage.putInt(index(index), (int) address);
	}

	@Override
	public int getAddress(int index) {
		return storage.getInt(index(index));
	}

	@Override
	public int capacity() {
		return capacity;
	}

}
