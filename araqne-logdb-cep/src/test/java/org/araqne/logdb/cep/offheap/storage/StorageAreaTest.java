package org.araqne.logdb.cep.offheap.storage;

import static org.junit.Assert.assertEquals;

import org.junit.Assert;
import org.junit.Test;

import sun.misc.Unsafe;

@SuppressWarnings("restriction")
public class StorageAreaTest {

//	@Test
//	public void allocateTest() {
//		Unsafe unsafe1 = UnsafeByteArrayStorageArea.getUnsafe();
//		Unsafe unsafe2 = UnsafeByteArrayStorageArea.getUnsafe();
//
//
//		long address = unsafe1.allocateMemory(100);
//		
//		unsafe1.putInt(address, 1234);
//		System.out.println(unsafe1.getInt(address));
//		
//		
//		System.out.println(unsafe2.getInt(address));
//		
//		unsafe2.freeMemory(address);
//		System.out.println(unsafe1.getInt(address));
//	}

	
	
	@Test
	public void byteArrayTest() {
		UnsafeByteArrayStorageArea storageArea = new UnsafeByteArrayStorageArea(50);

		storageArea.setValue(0, "test".getBytes());
		storageArea.setValue(10, "한글".getBytes());
		storageArea.setValue(20, "!@#$".getBytes());

		Assert.assertArrayEquals("test".getBytes(), storageArea.getValue(0));
		Assert.assertArrayEquals("한글".getBytes(), storageArea.getValue(10));
		Assert.assertArrayEquals("!@#$".getBytes(), storageArea.getValue(20));

		storageArea.close();
	}

	// @Test
	public void byteInflateTest() {
		UnsafeShortStorageArea storageArea = new UnsafeShortStorageArea(Integer.MAX_VALUE);
		storageArea.expansible(true);

		try {
			for (int i = 0; i < Integer.MAX_VALUE; i++) {
				storageArea.setValue(i, (short) i);
			}

			for (int i = 0; i < Integer.MAX_VALUE; i++) {
				System.out.println(i + " " + storageArea.getValue(i));
				assertEquals((short) i, (short) storageArea.getValue(i));
			}
		} finally {
			storageArea.close();
		}
	}

	@Test
	public void shortTest() {
		UnsafeShortStorageArea storageArea = new UnsafeShortStorageArea(50);
		storageArea.setValue(0, (short) 1234);
		storageArea.setValue(25, Short.MAX_VALUE);
		storageArea.setValue(49, Short.MIN_VALUE);

		assertEquals(1234, (short) storageArea.getValue(0));
		assertEquals(Short.MAX_VALUE, (short) storageArea.getValue(25));
		assertEquals(Short.MIN_VALUE, (short) storageArea.getValue(49));

		storageArea.close();

	}

	// @Test
	public void shortInflateTest() {
		UnsafeShortStorageArea storageArea = new UnsafeShortStorageArea(Integer.MAX_VALUE);
		storageArea.expansible(true);

		try {
			for (int i = 0; i < Integer.MAX_VALUE; i++) {
				storageArea.setValue(i, (short) i);
			}

			for (int i = 0; i < Integer.MAX_VALUE; i++) {
				System.out.println(i + " " + storageArea.getValue(i));
				assertEquals((short) i, (short) storageArea.getValue(i));
			}
		} finally {
			storageArea.close();
		}
	}

	// @Test
	public void integerTest() {
		UnsafeIntStorageArea storageArea = new UnsafeIntStorageArea(50);

		storageArea.setValue(0, 1234);
		storageArea.setValue(25, Integer.MAX_VALUE);
		storageArea.setValue(49, Integer.MIN_VALUE);

		assertEquals(1234, (int) storageArea.getValue(0));
		assertEquals(Integer.MAX_VALUE, (int) storageArea.getValue(25));
		assertEquals(Integer.MIN_VALUE, (int) storageArea.getValue(49));

		storageArea.close();
	}

	// @Test
	public void integerInflateTest() {
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

	// @Test
	public void longTest() {
		UnsafeIntStorageArea storageArea = new UnsafeIntStorageArea(50);

		storageArea.setValue(0, 1234);
		storageArea.setValue(25, Integer.MAX_VALUE);
		storageArea.setValue(49, Integer.MIN_VALUE);

		assertEquals(1234, (int) storageArea.getValue(0));
		assertEquals(Integer.MAX_VALUE, (int) storageArea.getValue(25));
		assertEquals(Integer.MIN_VALUE, (int) storageArea.getValue(49));

		storageArea.close();
	}

	// @Test
	public void longInflateTest() {
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
