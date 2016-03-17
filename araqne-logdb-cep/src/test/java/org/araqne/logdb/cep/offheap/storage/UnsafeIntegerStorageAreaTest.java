package org.araqne.logdb.cep.offheap.storage;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class UnsafeIntegerStorageAreaTest {

	@Test
	public void basicTest() {
		UnsafeIntStorageArea storageArea = new UnsafeIntStorageArea(50);

		storageArea.setValue(0, 1234);
		storageArea.setValue(25, Integer.MAX_VALUE);
		storageArea.setValue(49, Integer.MIN_VALUE);

		assertEquals(1234, (int) storageArea.getValue(0));
		assertEquals(Integer.MAX_VALUE, (int) storageArea.getValue(25));
		assertEquals(Integer.MIN_VALUE, (int) storageArea.getValue(49));
		
		storageArea.close();
		
	}

	//@Test
	public void basicTest2() {
		int capacity = Integer.MAX_VALUE;
		UnsafeIntStorageArea storageArea = new UnsafeIntStorageArea(capacity);
		storageArea.expansible(true);

		try {
			for (int i = 0; i < Integer.MAX_VALUE; i++) {
				storageArea.setValue(i, i);
			}

			for (int i = 0; i < Integer.MAX_VALUE; i++) {
				System.out.println(i + " " + storageArea.getValue(i));
				assertEquals(i, (int) storageArea.getValue(i));
			}
		} finally {
			storageArea.close();
		}

	}
}
