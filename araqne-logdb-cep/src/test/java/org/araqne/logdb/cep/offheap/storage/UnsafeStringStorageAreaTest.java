package org.araqne.logdb.cep.offheap.storage;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.araqne.logdb.cep.offheap.allocator.IntegerFirstFitAllocator;
import org.araqne.logdb.cep.offheap.storageArea.UnsafeStringStorageArea;
import org.junit.Test;

public class UnsafeStringStorageAreaTest {

	/*
	 * test method : setValue(), getValue()
	 */
	@Test
	public void basicTest() {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea(1000);

		storageArea.setValue(0, "1234");
		storageArea.setValue(20, "한글테스트");
		storageArea.setValue(40, "HelloWorld");
		storageArea.setValue(60, "~!@#$%^&*()_+|");

		assertEquals("1234", storageArea.getValue(0));
		assertEquals("한글테스트", storageArea.getValue(20));
		assertEquals("HelloWorld", storageArea.getValue(40));
		assertEquals("~!@#$%^&*()_+|", storageArea.getValue(60));

		storageArea.close();
	}

	/*
	 * test method : setAddress()
	 */
	@Test
	public void setAddressTest() throws IOException {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea(1000);

		storageArea.setAddress(0, 0);
		storageArea.setAddress(4, 1);
		storageArea.setAddress(8, 2);
		storageArea.setAddress(12, 3);

		assertEquals(0, storageArea.getAddress(0));
		assertEquals(1, storageArea.getAddress(4));
		assertEquals(2, storageArea.getAddress(8));
		assertEquals(3, storageArea.getAddress(12));

		storageArea.close();
	}

	/*
	 *  test method : allocate(), free()
	 */
	@Test
	public void allocatorTest() throws IOException {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea(10000);
		IntegerFirstFitAllocator allocator = new IntegerFirstFitAllocator(storageArea);

		String value = "abcdefghijklmn";
		int address = allocator.allocate(value.getBytes().length);
		storageArea.setValue(address, value);
		assertEquals(value, storageArea.getValue(address));
		allocator.free(address);
		storageArea.close();
	}

}
