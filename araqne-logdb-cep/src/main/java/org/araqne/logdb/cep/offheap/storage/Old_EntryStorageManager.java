//package org.araqne.logdb.cep.offheap.storage;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import org.araqne.logdb.cep.offheap.allocator.LongFixedSizeAllocator;
//import org.araqne.logdb.cep.offheap.engine.Entry;
//import org.araqne.logdb.cep.offheap.engine.serialize.Serialize;
//
//public class Old_EntryStorageManager<K, V> {
//	private int storageSize;
//	private Serialize<K> keySerializer;
//	private Serialize<V> valueSerializer;
//
//	private List<EntryStorageArea<K, V>> pageTable = new ArrayList<EntryStorageArea<K, V>>();
//	private LongFixedSizeAllocator allocator = new LongFixedSizeAllocator();
//
//	public Old_EntryStorageManager(int initialSize, Serialize<K> keySerializer, Serialize<V> valueSerializer) {
//		this.storageSize = initialSize;
//		this.keySerializer = keySerializer;
//		this.valueSerializer = valueSerializer;
//
//		EntryStorageArea<K, V> initialStorage = new EntryStorageArea<K, V>(storageSize, keySerializer, valueSerializer);
//
//		pageTable.add(initialStorage);
//		allocator.addStorage(initialStorage);
//	}
//
//	public Entry<K, V> get(long address) {
//		EntryStorageArea<K, V> storage = page(address);
//		if (storage == null)
//			return null;
//
//		return storage.getValue((int) address);
//	}
//
//	public V getValue(long address) {
//		EntryStorageArea<K, V> storage = page(address);
//		if (storage == null)
//			return null;
//
//		return storage.pickValue((int) address);
//	}
//
//	// allocate and put new entry
//	public long putEntry(Entry<K, V> entry) {
//		// encode
//		byte[] encodedKey = keySerializer.serialize(entry.getKey());
//		byte[] encodedValue = valueSerializer.serialize(entry.getValue());
//
//		// allocate
//		int requireSize = EntryStorageArea.sizeOf(encodedKey.length, encodedValue.length);
//		long address = allocator.allocate(requireSize);
//		// TODO inflate를 여기서 하거나 allocate에서 해야될듯
//
//		// set value
//		EntryStorageArea<K, V> storage = page(address);
//		if (storage == null)
//			return -1L;
//
//		storage.setEncodedValue((int) address, entry, encodedKey, encodedValue);
//		return address;
//	}
//
//	public long replaceValue(long address, V value, long timeoutTime) {
//		// encode
//		byte[] encodedValue = valueSerializer.serialize(value);
//
//		EntryStorageArea<K, V> storage = page(address);
//		// set value of entry
//		if (!storage.replaceValue((int) address, encodedValue, timeoutTime)) { // not
//			Entry<K, V> entry = storage.getValue((int) address);
//			entry.setValue(value);
//			entry.setTimeoutTime(timeoutTime);
//			address = putEntry(entry);
//		}
//		return address;
//	}
//
//	public long next(long address) {
//		EntryStorageArea<K, V> storage = page(address);
//		if (storage == null)
//			return 0L;
//
//		return storage.pickNext((int) address);
//	}
//
//	public boolean equalsHash(long address, int hash) {
//		EntryStorageArea<K, V> storage = page(address);
//		if (storage == null)
//			return false;
//
//		return storage.equalsHash((int) address, hash);
//	}
//
//	public boolean equalsKey(long address, Object key) {
//		EntryStorageArea<K, V> storage = page(address);
//		if (storage == null)
//			return false;
//
//		return storage.equalsKey((int) address, key);
//	}
//
//	public void setNext(long address, long next) {
//		EntryStorageArea<K, V> storage = page(address);
//		if (storage == null)
//			return;
//
//		storage.setNext((int) address, next);
//	}
//
//	public void remove(long address) {
//		allocator.free(address);
//	}
//
//	public long getTimeoutTime(long address) {
//		EntryStorageArea<K, V> storage = page(address);
//		if (storage == null)
//			return 0L;
//
//		return storage.pickTimeoutTime((int) address);
//	}
//
//	public int getHash(long address) {
//		EntryStorageArea<K, V> storage = page(address);
//		if (storage == null)
//			return 0;
//
//		return storage.pickHash((int) address);
//	}
//
//	private EntryStorageArea<K, V> page(long address) {
//		int pageNumber = (int) address >>> 32;
//		if (pageNumber > pageTable.size())
//			return null;
//		else
//			return pageTable.get(pageNumber);
//	}
//
//	public K getKey(long address) {
//		EntryStorageArea<K, V> storage = page(address);
//		if (storage == null)
//			return null;
//
//		return storage.pickKey((int) address);
//	}
//
//	public void clear() {
//		for (EntryStorageArea<K, V> storage : pageTable) {
//			storage.clear();
//		}
//
//		// TODO Allocate clear
//	}
//
//	public void close() {
//		for (EntryStorageArea<K, V> storage : pageTable) {
//			storage.close();
//		}
//
//		pageTable.clear();
//		//TODO allocate close
//	}
//
//	// // private int pageNo(long address) {
//	// // return (int) (address >> 32);
//	// // }
//	// //
//	// //
//	// // //address에 저장된 entry 읽음
//	// // //read -> decode
//	// // Entry<K, V> get(long address);
//	// //
//	// // //entry를 적당한 곳에 저장
//	// // //size of -> allocate -> encode
//	// // long set(Entry<K,V> entry);
//	// //
//	// // boolean equalsHash(long address, int hash);
//	// //
//	// // boolean equalsKey(long address, Object key);
//	// //
//	// // void replaceValue(long address, V value, long timeoutTime);
//	// //
//	// // long next(long address);
//	// //
//	// // long setValue(Entry<K,V> entry);
//	//
//	// private void addPage() {
//	// EntryStorageArea<K, V> storage = new EntryStorageArea<K, V>(storageSize,
//	// keySerializer, valueSerializer);
//	// pageTable.add(storage);
//	// // allocators.add(new IntegerFirstFitAllocator(storage));
//	// }
//
//	//
//	//
//
//	//
//
//}
