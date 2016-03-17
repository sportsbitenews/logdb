package org.araqne.logdb.cep.offheap.timeout;

import static org.junit.Assert.*;

import org.junit.Test;

public class TimeoutStorageTest {

	@Test
	public void test() {

		UnsafeTimeoutStorageArea storage = new UnsafeTimeoutStorageArea(20);

		long time = System.currentTimeMillis();
		long address = 1234L;

		storage.setValue(0, new TimeoutItem(time, address));
		TimeoutItem item = storage.getValue(0);

		assertEquals(time, item.getTime());
		assertEquals(address, item.getAddress());

		long time2 = Long.MAX_VALUE;
		long address2 = Long.MIN_VALUE;

		storage.setValue(19, new TimeoutItem(time2, address2));
		TimeoutItem item2 = storage.getValue(19);

		assertEquals(Long.MAX_VALUE, item2.getTime());
		assertEquals(Long.MIN_VALUE, item2.getAddress());

		storage.close();
	}

}
