package org.araqne.logdb.cep.offheap.storage;

import static org.junit.Assert.assertEquals;

import org.araqne.logdb.cep.offheap.storage.UnsafeLongStorageArea;
import org.junit.Test;

public class UnsafeLongStorageAreaTest {

	@Test
	public void basicTest() {
		UnsafeLongStorageArea storageArea = new UnsafeLongStorageArea(50);

		storageArea.setValue(0, 1234L);
		storageArea.setValue(25, Long.MAX_VALUE);
		storageArea.setValue(49, Long.MIN_VALUE);

		assertEquals(1234L, (long) storageArea.getValue(0));
		assertEquals(Long.MAX_VALUE, (long) storageArea.getValue(25));
		assertEquals(Long.MIN_VALUE, (long) storageArea.getValue(49));

		storageArea.close();
	}

	@Test
	public void basicTest2() {
		int size = 10000;
		UnsafeLongStorageArea storageArea = new UnsafeLongStorageArea(size);

		for (int i = 0; i < size; i++) {
			storageArea.setValue(i, i + 0L);
		}

		for (int i = 0; i < size; i++) {
			assertEquals(i + 0L, (long) storageArea.getValue(i));
		}

		storageArea.close();
	}

}
