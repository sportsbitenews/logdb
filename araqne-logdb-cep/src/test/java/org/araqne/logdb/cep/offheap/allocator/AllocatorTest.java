package org.araqne.logdb.cep.offheap.allocator;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.araqne.logdb.cep.offheap.allocator.IntegerFirstFitAllocator;
import org.araqne.logdb.cep.offheap.storageArea.UnsafeStringStorageArea;
import org.junit.Test;

public class AllocatorTest {

	@Test
	public void basicTest() throws IOException {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea();
		IntegerFirstFitAllocator allocator = new IntegerFirstFitAllocator(storageArea);

		String initMemState = allocator.toString();
		//System.out.println(allocator);


		String value = "abcd한글테스트1234";
		int address = allocator.allocate(value.getBytes().length);
		//System.out.println(allocator);

		System.out.println(allocator.space(address));
		
		storageArea.setValue(address, value);
		assertEquals(value, storageArea.getValue(address));
		//storageArea.remove(address);
		allocator.free(address);
		//System.out.println(allocator);

		assertEquals(initMemState, allocator.toString());
		//int address2 = allocator.allocate(10000);
		//assertEquals(-1, address2);
		storageArea.close();
	}

	@Test
	public void clearTest() {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea();
		IntegerFirstFitAllocator allocator = new IntegerFirstFitAllocator(storageArea);

		String initMemState = allocator.toString();

		String value = "abcd한글테스트1234";
		int address = allocator.allocate(value.getBytes().length * 2);
		storageArea.setValue(address, value);
		assertEquals(value, storageArea.getValue(address));
		//storageArea.remove(address);
		allocator.clear();
		assertEquals(initMemState, allocator.toString());

		storageArea.close();
	}

	@Test
	public void depth2Test1() throws IOException {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea(10000);
		IntegerFirstFitAllocator allocator = new IntegerFirstFitAllocator(storageArea);
		String initMemState = allocator.toString();

		String value1 = "ABCDEFGtest";
		String value2 = "12345678901";

		int address1 = allocator.allocate(value1.getBytes().length * 2);
		int address2 = allocator.allocate(value2.getBytes().length * 2);
		allocator.free(address1);
		allocator.free(address2);
		assertEquals(initMemState, allocator.toString());

		storageArea.close();
	}

	@Test
	public void depth2Test2() throws IOException {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea(10000);
		IntegerFirstFitAllocator allocator = new IntegerFirstFitAllocator(storageArea);
		String initMemState = allocator.toString();

		String value1 = "ABCDEFGtest";
		String value2 = "12345678901";

		int address1 = allocator.allocate(value1.getBytes().length * 2);
		int address2 = allocator.allocate(value2.getBytes().length * 2);
		allocator.free(address2);
		allocator.free(address1);
		assertEquals(initMemState, allocator.toString());

		storageArea.close();
	}

	@Test
	public void depth2Test3() throws IOException {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea(10000);
		IntegerFirstFitAllocator allocator = new IntegerFirstFitAllocator(storageArea);
		String initMemState = allocator.toString();

		String value1 = "ABCDEFGtest";
		String value2 = "12345678901";

		int address1 = allocator.allocate(value1.getBytes().length * 2);
		allocator.free(address1);
		int address2 = allocator.allocate(value2.getBytes().length * 2);
		allocator.free(address2);
		assertEquals(initMemState, allocator.toString());

		storageArea.close();
	}

	@Test
	public void depth2Test4() throws IOException {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea(10000);
		IntegerFirstFitAllocator allocator = new IntegerFirstFitAllocator(storageArea);
		String initMemState = allocator.toString();

		String value1 = "ABCDEFGtest";
		String value2 = "12345678901";

		int address2 = allocator.allocate(value2.getBytes().length * 2);
		int address1 = allocator.allocate(value1.getBytes().length * 2);
		allocator.free(address1);
		allocator.free(address2);
		assertEquals(initMemState, allocator.toString());

		storageArea.close();
	}

	@Test
	public void depth2Test5() throws IOException {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea(10000);
		IntegerFirstFitAllocator allocator = new IntegerFirstFitAllocator(storageArea);
		String initMemState = allocator.toString();

		String value1 = "ABCDEFGtest";
		String value2 = "12345678901";

		int address2 = allocator.allocate(value2.getBytes().length * 2);
		int address1 = allocator.allocate(value1.getBytes().length * 2);
		allocator.free(address2);
		allocator.free(address1);
		assertEquals(initMemState, allocator.toString());

		storageArea.close();
	}

	@Test
	public void depth2Test6() throws IOException {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea(10000);
		IntegerFirstFitAllocator allocator = new IntegerFirstFitAllocator(storageArea);
		String initMemState = allocator.toString();

		String value1 = "ABCDEFGtest";
		String value2 = "12345678901";

		int address2 = allocator.allocate(value2.getBytes().length * 2);
		allocator.free(address2);
		int address1 = allocator.allocate(value1.getBytes().length * 2);
		allocator.free(address1);
		assertEquals(initMemState, allocator.toString());

		storageArea.close();
	}

	@Test
	public void depth3Test1() throws IOException {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea(10000);
		IntegerFirstFitAllocator allocator = new IntegerFirstFitAllocator(storageArea);
		String initMemState = allocator.toString();

		String value1 = "ABCDEFGtest";
		String value2 = "한글";
		String value3 = "12345678901";

		int address1 = allocator.allocate(value1.getBytes().length * 2);
		int address2 = allocator.allocate(value2.getBytes().length * 2);
		int address3 = allocator.allocate(value3.getBytes().length * 2);
		allocator.free(address1);
		allocator.free(address2);
		allocator.free(address3);
		assertEquals(initMemState, allocator.toString());

		storageArea.close();
	}

	@Test
	public void depth3Test2() throws IOException {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea(10000);
		IntegerFirstFitAllocator allocator = new IntegerFirstFitAllocator(storageArea);
		String initMemState = allocator.toString();

		String value1 = "ABCDEFGtest";
		String value2 = "한글";
		String value3 = "12345678901";

		int address1 = allocator.allocate(value1.getBytes().length * 2);
		int address2 = allocator.allocate(value2.getBytes().length * 2);
		int address3 = allocator.allocate(value3.getBytes().length * 2);
		allocator.free(address1);
		allocator.free(address3);
		allocator.free(address2);
		assertEquals(initMemState, allocator.toString());

		storageArea.close();
	}

	@Test
	public void depth3Test3() throws IOException {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea(10000);
		IntegerFirstFitAllocator allocator = new IntegerFirstFitAllocator(storageArea);
		String initMemState = allocator.toString();

		String value1 = "ABCDEFGtest";
		String value2 = "한글";
		String value3 = "12345678901";
		int address1 = allocator.allocate(value1.getBytes().length * 2);
		int address2 = allocator.allocate(value2.getBytes().length * 2);
		int address3 = allocator.allocate(value3.getBytes().length * 2);

		allocator.free(address2);
		allocator.free(address1);
		allocator.free(address3);
		assertEquals(initMemState, allocator.toString());

		storageArea.close();
	}

	@Test
	public void depth3Test4() throws IOException {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea(10000);
		IntegerFirstFitAllocator allocator = new IntegerFirstFitAllocator(storageArea);
		String initMemState = allocator.toString();

		String value1 = "ABCDEFGtest";
		String value2 = "한글";
		String value3 = "12345678901";

		int address1 = allocator.allocate(value1.getBytes().length * 2);
		int address2 = allocator.allocate(value2.getBytes().length * 2);
		int address3 = allocator.allocate(value3.getBytes().length * 2);
		allocator.free(address2);
		allocator.free(address3);
		allocator.free(address1);
		assertEquals(initMemState, allocator.toString());

		storageArea.close();
	}

	@Test
	public void depth3Test5() throws IOException {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea(10000);
		IntegerFirstFitAllocator allocator = new IntegerFirstFitAllocator(storageArea);
		String initMemState = allocator.toString();

		String value1 = "ABCDEFGtest";
		String value2 = "한글";
		String value3 = "12345678901";

		int address1 = allocator.allocate(value1.getBytes().length * 2);
		int address2 = allocator.allocate(value2.getBytes().length * 2);
		int address3 = allocator.allocate(value3.getBytes().length * 2);
		allocator.free(address3);
		allocator.free(address1);
		allocator.free(address2);
		assertEquals(initMemState, allocator.toString());

		storageArea.close();
	}

	@Test
	public void depth3Test6() throws IOException {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea(10000);
		IntegerFirstFitAllocator allocator = new IntegerFirstFitAllocator(storageArea);
		String initMemState = allocator.toString();

		String value1 = "ABCDEFGtest";
		String value2 = "한글";
		String value3 = "12345678901";

		int address1 = allocator.allocate(value1.getBytes().length * 2);
		int address2 = allocator.allocate(value2.getBytes().length * 2);
		int address3 = allocator.allocate(value3.getBytes().length * 2);
		allocator.free(address3);
		allocator.free(address2);
		allocator.free(address1);
		assertEquals(initMemState, allocator.toString());

		storageArea.close();
	}

	@Test
	public void depth3Test7() throws IOException {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea(10000);
		IntegerFirstFitAllocator allocator = new IntegerFirstFitAllocator(storageArea);
		String initMemState = allocator.toString();

		String value1 = "ABCDEFGtest";
		String value2 = "한글";
		String value3 = "12345678901";

		int address1 = allocator.allocate(value1.getBytes().length * 2);
		int address2 = allocator.allocate(value2.getBytes().length * 2);
		allocator.free(address1);
		int address3 = allocator.allocate(value3.getBytes().length * 2);
		allocator.free(address2);
		allocator.free(address3);
		assertEquals(initMemState, allocator.toString());

		storageArea.close();
	}

	@Test
	public void depth3Test8() throws IOException {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea(10000);
		IntegerFirstFitAllocator allocator = new IntegerFirstFitAllocator(storageArea);
		String initMemState = allocator.toString();

		String value1 = "ABCDEFGtest";
		String value2 = "한글";
		String value3 = "12345678901";

		int address1 = allocator.allocate(value1.getBytes().length * 2);
		int address2 = allocator.allocate(value2.getBytes().length * 2);
		allocator.free(address1);
		int address3 = allocator.allocate(value3.getBytes().length * 2);
		allocator.free(address3);
		allocator.free(address2);
		assertEquals(initMemState, allocator.toString());

		storageArea.close();
	}

	@Test
	public void depth3Test9() throws IOException {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea(10000);
		IntegerFirstFitAllocator allocator = new IntegerFirstFitAllocator(storageArea);
		String initMemState = allocator.toString();

		String value1 = "ABCDEFGtest";
		String value2 = "한글";
		String value3 = "12345678901";

		int address1 = allocator.allocate(value1.getBytes().length * 2);
		int address2 = allocator.allocate(value2.getBytes().length * 2);
		allocator.free(address1);
		allocator.free(address2);
		int address3 = allocator.allocate(value3.getBytes().length * 2);
		allocator.free(address3);
		assertEquals(initMemState, allocator.toString());

		storageArea.close();
	}

	@Test
	public void depth3Test10() throws IOException {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea(10000);
		IntegerFirstFitAllocator allocator = new IntegerFirstFitAllocator(storageArea);
		String initMemState = allocator.toString();

		String value1 = "ABCDEFGtest";
		String value2 = "한글";
		String value3 = "12345678901";

		int address1 = allocator.allocate(value1.getBytes().length * 2);
		int address2 = allocator.allocate(value2.getBytes().length * 2);
		allocator.free(address2);
		int address3 = allocator.allocate(value3.getBytes().length * 2);
		allocator.free(address1);
		allocator.free(address3);
		assertEquals(initMemState, allocator.toString());

		storageArea.close();
	}

	@Test
	public void depth3Test11() throws IOException {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea(10000);
		IntegerFirstFitAllocator allocator = new IntegerFirstFitAllocator(storageArea);
		String initMemState = allocator.toString();

		String value1 = "ABCDEFGtest";
		String value2 = "한글";
		String value3 = "12345678901";

		int address1 = allocator.allocate(value1.getBytes().length * 2);
		int address2 = allocator.allocate(value2.getBytes().length * 2);
		allocator.free(address2);
		int address3 = allocator.allocate(value3.getBytes().length * 2);
		allocator.free(address3);
		allocator.free(address1);
		assertEquals(initMemState, allocator.toString());

		storageArea.close();
	}

	@Test
	public void depth3Test12() throws IOException {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea(10000);
		IntegerFirstFitAllocator allocator = new IntegerFirstFitAllocator(storageArea);
		String initMemState = allocator.toString();

		String value1 = "ABCDEFGtest";
		String value2 = "한글";
		String value3 = "12345678901";

		int address1 = allocator.allocate(value1.getBytes().length * 2);
		int address2 = allocator.allocate(value2.getBytes().length * 2);
		allocator.free(address2);
		allocator.free(address1);
		int address3 = allocator.allocate(value3.getBytes().length * 2);
		allocator.free(address3);
		assertEquals(initMemState, allocator.toString());

		storageArea.close();
	}

	@Test
	public void depth3Test13() throws IOException {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea(10000);
		IntegerFirstFitAllocator allocator = new IntegerFirstFitAllocator(storageArea);
		String initMemState = allocator.toString();

		String value1 = "ABCDEFGtest";
		String value2 = "한글";
		String value3 = "12345678901";

		int address1 = allocator.allocate(value1.getBytes().length * 2);
		int address3 = allocator.allocate(value3.getBytes().length * 2);
		int address2 = allocator.allocate(value2.getBytes().length * 2);
		allocator.free(address1);
		allocator.free(address2);
		allocator.free(address3);
		assertEquals(initMemState, allocator.toString());

		storageArea.close();
	}

	@Test
	public void depth3Test14() throws IOException {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea(10000);
		IntegerFirstFitAllocator allocator = new IntegerFirstFitAllocator(storageArea);
		String initMemState = allocator.toString();

		String value1 = "ABCDEFGtest";
		String value2 = "한글";
		String value3 = "12345678901";

		int address1 = allocator.allocate(value1.getBytes().length * 2);
		int address3 = allocator.allocate(value3.getBytes().length * 2);
		int address2 = allocator.allocate(value2.getBytes().length * 2);
		allocator.free(address1);
		allocator.free(address3);
		allocator.free(address2);
		assertEquals(initMemState, allocator.toString());

		storageArea.close();
	}

	@Test
	public void depth3Test15() throws IOException {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea(10000);
		IntegerFirstFitAllocator allocator = new IntegerFirstFitAllocator(storageArea);
		String initMemState = allocator.toString();

		String value1 = "ABCDEFGtest";
		String value2 = "한글";
		String value3 = "12345678901";
		int address1 = allocator.allocate(value1.getBytes().length * 2);
		int address3 = allocator.allocate(value3.getBytes().length * 2);
		int address2 = allocator.allocate(value2.getBytes().length * 2);
		allocator.free(address2);
		allocator.free(address1);
		allocator.free(address3);
		assertEquals(initMemState, allocator.toString());

		storageArea.close();
	}

	@Test
	public void depth3Test16() throws IOException {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea(10000);
		IntegerFirstFitAllocator allocator = new IntegerFirstFitAllocator(storageArea);
		String initMemState = allocator.toString();

		String value1 = "ABCDEFGtest";
		String value2 = "한글";
		String value3 = "12345678901";

		int address1 = allocator.allocate(value1.getBytes().length * 2);
		int address3 = allocator.allocate(value3.getBytes().length * 2);
		int address2 = allocator.allocate(value2.getBytes().length * 2);
		allocator.free(address2);
		allocator.free(address3);
		allocator.free(address1);
		assertEquals(initMemState, allocator.toString());

		storageArea.close();
	}

	@Test
	public void depth3Test17() throws IOException {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea(10000);
		IntegerFirstFitAllocator allocator = new IntegerFirstFitAllocator(storageArea);
		String initMemState = allocator.toString();

		String value1 = "ABCDEFGtest";
		String value2 = "한글";
		String value3 = "12345678901";

		int address1 = allocator.allocate(value1.getBytes().length * 2);
		int address3 = allocator.allocate(value3.getBytes().length * 2);
		int address2 = allocator.allocate(value2.getBytes().length * 2);
		allocator.free(address3);
		allocator.free(address1);
		allocator.free(address2);
		assertEquals(initMemState, allocator.toString());

		storageArea.close();
	}

	@Test
	public void depth3Test18() throws IOException {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea(10000);
		IntegerFirstFitAllocator allocator = new IntegerFirstFitAllocator(storageArea);
		String initMemState = allocator.toString();

		String value1 = "ABCDEFGtest";
		String value2 = "한글";
		String value3 = "12345678901";

		int address1 = allocator.allocate(value1.getBytes().length * 2);
		int address2 = allocator.allocate(value2.getBytes().length * 2);
		int address3 = allocator.allocate(value3.getBytes().length * 2);
		allocator.free(address3);
		allocator.free(address2);
		allocator.free(address1);
		assertEquals(initMemState, allocator.toString());

		storageArea.close();
	}

	@Test
	public void depth3Test19() throws IOException {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea(10000);
		IntegerFirstFitAllocator allocator = new IntegerFirstFitAllocator(storageArea);
		String initMemState = allocator.toString();

		String value1 = "ABCDEFGtest";
		String value2 = "한글";
		String value3 = "12345678901";

		int address1 = allocator.allocate(value1.getBytes().length * 2);
		int address3 = allocator.allocate(value3.getBytes().length * 2);
		allocator.free(address1);
		int address2 = allocator.allocate(value2.getBytes().length * 2);
		allocator.free(address2);
		allocator.free(address3);
		assertEquals(initMemState, allocator.toString());

		storageArea.close();
	}

	@Test
	public void depth3Test20() throws IOException {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea(10000);
		IntegerFirstFitAllocator allocator = new IntegerFirstFitAllocator(storageArea);
		String initMemState = allocator.toString();

		String value1 = "ABCDEFGtest";
		String value2 = "한글";
		String value3 = "12345678901";

		int address1 = allocator.allocate(value1.getBytes().length * 2);
		int address3 = allocator.allocate(value3.getBytes().length * 2);
		allocator.free(address1);
		int address2 = allocator.allocate(value2.getBytes().length * 2);
		allocator.free(address3);
		allocator.free(address2);
		assertEquals(initMemState, allocator.toString());

		storageArea.close();
	}

	@Test
	public void depth3Test21() throws IOException {
		UnsafeStringStorageArea storageArea = new UnsafeStringStorageArea(10000);
		IntegerFirstFitAllocator allocator = new IntegerFirstFitAllocator(storageArea);
		String initMemState = allocator.toString();

		String value1 = "ABCDEFGtest";
		String value2 = "한글";
		String value3 = "12345678901";

		int address1 = allocator.allocate(value1.getBytes().length * 2);
		int address3 = allocator.allocate(value3.getBytes().length * 2);
		allocator.free(address1);
		allocator.free(address3);
		int address2 = allocator.allocate(value2.getBytes().length * 2);
		allocator.free(address2);
		assertEquals(initMemState, allocator.toString());

		storageArea.close();
	}

}
