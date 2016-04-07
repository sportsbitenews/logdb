package org.araqne.logdb.cep.offheap.storageArea;

/*
 * unsafe short array ~= short[]	 
 */
@SuppressWarnings("restriction")
public class UnsafeShortStorageArea extends AbsractUnsafeStorageArea<Short> {

	public UnsafeShortStorageArea() {
		super(2);
	}

	public UnsafeShortStorageArea(int initialCapacity) {
		super(2, initialCapacity);
	}

	@Override
	public void setValue(int index, Short value) {
		storage.putShort(index(index), value);
	}

	@Override
	public Short getValue(int index) {
		return storage.getShort(index(index));
	}

}
