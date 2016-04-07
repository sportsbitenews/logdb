package org.araqne.logdb.cep.offheap.storageArea;

@SuppressWarnings("restriction")
public class UnsafeLongStorageArea extends AbsractUnsafeStorageArea<Long> {

	public UnsafeLongStorageArea() {
		super(8);
	}

	public UnsafeLongStorageArea(int initialCapacity) {
		super(8, initialCapacity);
	}

	@Override
	public void setValue(int index, Long value) {
		storage.putLong(index(index), value);
	}

	@Override
	public Long getValue(int index) {
		return storage.getLong(index(index));
	}

}
