package org.araqne.logdb.cep.offheap.storage;

import static org.junit.Assert.assertEquals;

import org.araqne.logdb.cep.offheap.storageArea.UnsafeIntStorageArea;
import org.araqne.logdb.cep.offheap.storageArea.UnsafeLongStorageArea;
import org.junit.Test;

public class UnsafeLongStorageAreaTest {

	/*
	 * test method : setValue(), getValue()
	 */
	@Test
	public void basicTest() {
		int size = 10000;
		UnsafeLongStorageArea storageArea = new UnsafeLongStorageArea(size);

		for (int i = 0; i < size; i++) {
			storageArea.setValue(i, i + 0L);
		}

		for (int i = 0; i < size; i++) {
			assertEquals(i + 0L, (long) storageArea.getValue(i));
		}

		storageArea.setValue(0, 1234L);
		storageArea.setValue(25, Long.MAX_VALUE);
		storageArea.setValue(49, Long.MIN_VALUE);

		assertEquals(1234L, (long) storageArea.getValue(0));
		assertEquals(Long.MAX_VALUE, (long) storageArea.getValue(25));
		assertEquals(Long.MIN_VALUE, (long) storageArea.getValue(49));

		storageArea.close();
	}

	/*
	 * test exception : index out of bounds
	 */
	@Test(expected = IllegalStateException.class)
	public void exceptionTest() {
		UnsafeLongStorageArea storageArea = new UnsafeLongStorageArea(10);
		storageArea.setValue(10, 0L);
	}

	/*
	 * test method : inflate()
	 */
	@Test
	public void longInflateTest() {
		int capacity = 1024;
		UnsafeIntStorageArea storageArea = new UnsafeIntStorageArea(capacity);
		storageArea.expansible(true);

		try {
			for (int i = 0; i < capacity * capacity; i++) {
				storageArea.setValue(i, i);
			}

			for (int i = 0; i < capacity * capacity; i++) {
				assertEquals(i, (int) storageArea.getValue(i));
			}
		} finally {
			storageArea.close();
		}
	}

}
