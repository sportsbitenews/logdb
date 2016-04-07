package org.araqne.logdb.cep.offheap.storageArea;

@SuppressWarnings("restriction")
public class UnsafeIntStorageArea extends AbsractUnsafeStorageArea<Integer> {

	public UnsafeIntStorageArea() {
		super(4);
	}

	public UnsafeIntStorageArea(int initialCapacity) {
		super(4, initialCapacity);
	}

	@Override
	public void setValue(int index, Integer value) {
		storage.putInt(index(index), value);
	}

	@Override
	public Integer getValue(int index) {
		return storage.getInt(index(index));
	}

}
