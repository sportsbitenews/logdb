package org.araqne.storage.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.araqne.storage.api.RCDirectBufferManager.ExceedPoolSizeLimitException;
import org.junit.BeforeClass;
import org.junit.Test;

public class RCDirectBufferManagerImplTest {
	private static RCDirectBufferManagerImpl manager;

	private static String poolName1 = "pool1";
	private static String poolName2 = "pool2";
	private static String poolName3 = "pool3";

	private static String usageName1 = "usage1";
	private static String usageName2 = "usage2";
	private static String usageName3 = "usage3";

	private static long usingMemorySize1;
	private static long usingMemorySize2;
	private static long usingMemorySize3;

	private static long availableMemorySize1;
	private static long availableMemorySize2;
	private static long availableMemorySize3;

	@BeforeClass
	public static void before() throws ExceedPoolSizeLimitException {
		manager = RCDirectBufferManagerImpl.getTestManager();

		manager.setMemoryLimitOfPool(poolName1, 2L);
		manager.allocateDirect(1, poolName1, usageName1);
		manager.allocateDirect(1, poolName1, usageName2);
		usingMemorySize1 = 2L;
		availableMemorySize1 = 0L;

		manager.setMemoryLimitOfPool(poolName2, 3L);
		manager.allocateDirect(1, poolName2, usageName1);
		usingMemorySize2 = 1L;
		availableMemorySize2 = 2L;

		manager.allocateDirect(1, poolName3, usageName1);
		manager.allocateDirect(1, poolName3, usageName2);
		manager.allocateDirect(1, poolName3, usageName3);
		manager.allocateDirect(1, poolName3, usageName1);
		manager.allocateDirect(1, poolName3, usageName1);
		usingMemorySize3 = 5L;
		availableMemorySize3 = Long.MAX_VALUE - usingMemorySize3;
	}

	@Test
	public void poolSizeLimitTest() {
		try {
			manager.allocateDirect(1, poolName1, usageName3);
		} catch (ExceedPoolSizeLimitException e) {
			return;
		}

		// Should Not reach.
		assertTrue(false);
	}

	@Test
	public void getUsingPoolSizeTest() {
		assertEquals(usingMemorySize1, manager.getUsingPoolSize(poolName1));
		assertEquals(usingMemorySize2, manager.getUsingPoolSize(poolName2));
		assertEquals(usingMemorySize3, manager.getUsingPoolSize(poolName3));
	}

	@Test
	public void getAvailablePoolSizeTest() {
		assertEquals(availableMemorySize1, manager.getAvailablePoolSize(poolName1));
		assertEquals(availableMemorySize2, manager.getAvailablePoolSize(poolName2));
		assertEquals(availableMemorySize3, manager.getAvailablePoolSize(poolName3));
	}
}
