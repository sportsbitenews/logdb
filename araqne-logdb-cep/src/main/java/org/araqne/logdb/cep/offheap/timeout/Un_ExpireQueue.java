//package org.araqne.logdb.cep.offheap.timeout;
//
//
//public class Un_ExpireQueue  {
//	private int currentSize;
//	private UnsafeTimeoutStorageArea storage = new UnsafeTimeoutStorageArea();
//	
//	private Un_ExpireQueue() {
//		currentSize = 0;
//		storage.expansible(true);
//	}
//
//	public int size() {
//		return currentSize;
//	}
//
//	public void close() {
//		storage.close();
//	}
//
//	public void clear() {
//		storage.close();
//		storage =  new UnsafeTimeoutStorageArea();
//		currentSize = 0;
//	}
//
//	public TimeoutItem get(int i) {
//		return storage.getValue(i);
//	}
//	
//	public boolean add(TimeoutItem item) {
//		int hole = ++currentSize;
//		storage.setValue(0, item);
//
//		for (; compare(item, storage.getValue(hole / 2)) < 0; hole /= 2)
//			storage.setValue(hole, storage.getValue(hole / 2));
//
//		storage.setValue(hole, item);
//		return true;
//	}
//
//	private int compare(TimeoutItem lhs, TimeoutItem rhs) {
//		return Long.compare(lhs.getTime(), rhs.getTime());
//	}
//
//	public TimeoutItem peek() { // don`t remove
//		if (currentSize < 1)
//			return null;
//
//		return storage.getValue(1);
//	}
//
//	public TimeoutItem remove() {
//		TimeoutItem minItem = peek();
//		storage.setValue(1, storage.getValue(currentSize--));
//		percolateDown(1);
//		
//		return minItem;
//	}
//	
//	private void percolateDown(int hole) {
//		int child = hole * 2;
//		TimeoutItem tmp = storage.getValue(hole);
//		
//		for(; hole * 2 <= currentSize; hole = child) {
//			child = hole * 2;
//			if(child != currentSize && compare(storage.getValue(child + 1), storage.getValue(child)) < 0)
//				child++;
//			
//			if(compare(storage.getValue(child), tmp) < 0) 
//				storage.setValue(hole, storage.getValue(child));
//			else
//				break;
//		}
//		
//		storage.setValue(hole, tmp);
//	}
//
//}
