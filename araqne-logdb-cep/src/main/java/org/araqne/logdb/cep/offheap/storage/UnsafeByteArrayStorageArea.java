package org.araqne.logdb.cep.offheap.storage;

import org.araqne.logdb.cep.offheap.allocator.AllocableArea;

@SuppressWarnings("restriction")
public class UnsafeByteArrayStorageArea extends AbsractUnsafeStorageArea<byte[]> implements AllocableArea<byte[]> {

	public UnsafeByteArrayStorageArea() {
		super(1);
	}

	public UnsafeByteArrayStorageArea(int initialCapacity) {
		super(1, initialCapacity);
	}
	
	@Override
	public void setValue(int index, byte[] bytes) {
		// XXX value size는 항상 integer? 
		//byte 단위로 (signal bit 사용하면 공간 절약 but 읽고 쓰는 시간 추가 필요)
		storage.putInt(index(index), bytes.length);
		for (int i = 0; i < bytes.length; i++) {
			storage.putByte(index(index) + 4 + i, bytes[i]); // 4 = size of integer
		}
	}

	@Override
	public byte[] getValue(int index) {
		int valueLength = storage.getInt(index(index));
		byte[] ret = new byte[valueLength];
		for (int i = 0; i < valueLength; i++) {
			ret[i] = storage.getByte(index(index) + 4 + i); //4 = size of integer
		}

		return ret;
	}
	
	@Override
	public void remove(int index) {
	
	}
	
	@Override
	public void setAddress(int index, int address) {
		storage.putInt(index(index), (int) address);
	}

	@Override
	public int getAddress(int index) {
		return  storage.getInt(index(index));// + (offset * addressSize)));
	}

	@Override
	public int capacity() {
		return capacity;
	}

}
