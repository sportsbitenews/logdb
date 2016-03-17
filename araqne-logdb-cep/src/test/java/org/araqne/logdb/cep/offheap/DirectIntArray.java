package org.araqne.logdb.cep.offheap;

import static org.junit.Assert.*;

import java.lang.reflect.Field;

import org.junit.Test;

import sun.misc.Unsafe;

@SuppressWarnings("restriction")
public class DirectIntArray {

	public static Unsafe getUnsafe() {
		try {
			Field f = Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			return (Unsafe) f.get(null);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	private long index(long startIndex, long offset) {
		return startIndex + offset * 4;
	}

	@Test
	public void unsafeTest() {
		Unsafe unsafe = getUnsafe();
		int size = 1000;
		long startIndex = unsafe.allocateMemory(size * 4);
		unsafe.setMemory(startIndex, size * 4, (byte) 0);

		unsafe.putInt(index(startIndex, 0L), 10);
		assertEquals(10, unsafe.getInt(index(startIndex, 0L)));
		unsafe.freeMemory(startIndex);
	}
}
