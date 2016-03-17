package org.araqne.logdb.cep.offheap.timeout;
//package org.araqne.logdb.cep.offheap.timeout;
//
//import org.araqne.logdb.cep.offheap.storage.AbsractUnsafeStorageArea;
//
//@SuppressWarnings("restriction")
//public class UnsafeExpiretStorageArea extends AbsractUnsafeStorageArea<TimeoutItem> {
//
//	public UnsafeExpiretStorageArea() {
//		super(16);
//	}
//
//	public UnsafeExpiretStorageArea(int initialCapacity) {
//		super(16, initialCapacity);
//	}
//
//	@Override
//	public void setValue(int index, TimeoutItem value) {
//		storage.putLong(index(index), value.getTime());
//		storage.putLong(index(index) + 8, value.getAddress());
//	}
//
//	@Override
//	public TimeoutItem getValue(int index) {
//		long time = storage.getLong(index(index));
//		long address = storage.getLong(index(index) + 8);
//
//		return new TimeoutItem(time, address);
//	}
//
//}
