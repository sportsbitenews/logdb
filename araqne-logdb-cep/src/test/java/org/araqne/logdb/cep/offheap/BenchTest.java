package org.araqne.logdb.cep.offheap;
import static org.junit.Assert.assertEquals;

import java.util.Date;

import org.araqne.logdb.Row;
import org.araqne.logdb.cep.EventContext;
import org.araqne.logdb.cep.EventKey;
import org.araqne.logdb.cep.offheap.TimeoutMap;
import org.araqne.logdb.cep.offheap.factory.ConcurrentTimeoutMapFactory;
import org.araqne.logdb.cep.offheap.factory.TimeoutMapFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BenchTest {

	private TimeoutMap<String, String> map1;
	private TimeoutMap<String, String> map2;
	private TimeoutMap<EventKey, EventContext> map3;

	private final static int size = 300000;
	private final boolean test = false;

	@Before
	public void onStart() {
		map1 = TimeoutMapFactory.string(1024 * 1024 * 32, 256).map();
		map2 = ConcurrentTimeoutMapFactory.string(5, 1024 * 1024, 256).map();
		map3 = ConcurrentTimeoutMapFactory.event(5, 1024 * 1024, 512).map();
	}

	@After
	public void onClose() {
		map1.close();
		map2.close();
		map3.close();
	}

	/*
	 * string, off heap map - putIfAbsent(), replace(), get(), remove()
	 */
	@Test
	public void benchTest1() {
		if (!test)
			return;

		System.out.println("OffHeap<String,String> cnt = " + size);
		long s = System.currentTimeMillis();
		for (int i = 0; i < size; i++) {
			assertEquals(true, map1.putIfAbsent("" + i, "v", null, 0L, 0L));
		}
		long e = System.currentTimeMillis() - s;
		System.out.println("putIfAbsent :" + e + "ms, " + size * 1000L / e + "tps");

		s = System.currentTimeMillis();
		for (int i = 0; i < size; i++) {
			assertEquals(true, map1.replace("" + i, "v", "" + i, null, 0L));
		}
		e = System.currentTimeMillis() - s;
		System.out.println("replace :" + e + "ms, " + size * 1000L / e + "tps");

		s = System.currentTimeMillis();
		for (int i = 0; i < size; i++) {
			assertEquals("" + i, map1.get("" + i));
		}
		e = System.currentTimeMillis() - s;
		System.out.println("get :" + e + "ms, " + size * 1000L / e + "tps");

		s = System.currentTimeMillis();
		for (int i = 0; i < size; i++) {
			assertEquals(true, map1.remove("" + i));
		}
		e = System.currentTimeMillis() - s;
		System.out.println("remove :" + e + "ms, " + size * 1000L / e + "tps");

		map1.clear();
	}

	/*
	 * string, concurrent off heap map - putIfAbsent(), replace(), get(),
	 * remove()
	 */
	@Test
	public void benchTest2() {
		if (!test)
			return;

		System.out.println("Concurrent OffHeap<String,String> cnt = " + size);
		long s = System.currentTimeMillis();
		for (int i = 0; i < size; i++) {
			assertEquals(true, map2.putIfAbsent("" + i, "v", null, 0L, 0L));
		}
		long e = System.currentTimeMillis() - s;
		System.out.println("putIfAbsent :" + e + "ms, " + size * 1000L / e + "tps");

		s = System.currentTimeMillis();
		for (int i = 0; i < size; i++) {
			assertEquals(true, map2.replace("" + i, "v", "n" + i, null, 0L));
		}
		e = System.currentTimeMillis() - s;
		System.out.println("replace :" + e + "ms, " + size * 1000L / e + "tps");

		s = System.currentTimeMillis();
		for (int i = 0; i < size; i++) {
			assertEquals("n" + i, map2.get("" + i));
		}
		e = System.currentTimeMillis() - s;
		System.out.println("get :" + e + "ms, " + size * 1000L / e + "tps");

		s = System.currentTimeMillis();
		for (int i = 0; i < size; i++) {
			assertEquals(true, map2.remove("" + i));
		}
		e = System.currentTimeMillis() - s;
		System.out.println("remove :" + e + "ms, " + size * 1000L / e + "tps");

		map2.clear();
	}

	/*
	 * Event, concurrent off heap map - putIfAbsent(), replace(), get(),
	 * remove()
	 */
	@Test
	public void benchTest3() {
		if (!test)
			return;

		Row row = new Row();
		row.put("_table", "t");
		row.put("_id", 999999);
		row.put("_time", "2016-03-10 19:47:07+0900");
		row.put("key", "999999");

		Row row2 = new Row();
		row2.put("_table", "t2");
		row2.put("_id", 999998);
		row2.put("_time", "2016-03-11 19:47:07+0900");
		row2.put("key", "999998");

		System.out.println("Concurrent OffHeap<EventKey,EventContext> cnt = " + size);
		long s = System.currentTimeMillis();
		for (int i = 0; i < size; i++) {
			EventKey key = new EventKey("topic", i + "", null);
			EventContext value = new EventContext(key, 0L, 0L, 0L, 10);
			value.getCounter().set(1);
			//value.addRow(row);
			//value.setVariable("ip", "127.0.0.1");
			//value.setVariable("name", "kim");
			map3.putIfAbsent(key, value, null, 0L, 0L);
		}
		long e = System.currentTimeMillis() - s;
		System.out.println("putIfAbsent :" + e + "ms, " + size * 1000L / e + "tps");

		s = System.currentTimeMillis();
		for (int i = 0; i < size; i++) {
			EventKey key = new EventKey("topic", i + "", null);
			EventContext value = new EventContext(key, 0L, 0L, 0L, 10);
			value.getCounter().set(1);
			value.addRow(row);
			value.setVariable("ip", "127.0.0.1");
			value.setVariable("name", "kim");

			EventContext newValue = new EventContext(key, new Date().getTime(), 0L, 0L, 10);
			newValue.getCounter().set(1);
			newValue.addRow(row);
			newValue.addRow(row2);
			newValue.setVariable("ip", "127.0.0.1");
			newValue.setVariable("name", "kim");
			newValue.setVariable("count", 3);

			map3.replace(key, value, newValue, null, 0L);
		}
		e = System.currentTimeMillis() - s;
		System.out.println("replace :" + e + "ms, " + size * 1000L / e + "tps");

		s = System.currentTimeMillis();
		for (int i = 0; i < size; i++) {
			EventKey key = new EventKey("topic", i + "", null);
			map3.get(key);
		}
		e = System.currentTimeMillis() - s;
		System.out.println("get :" + e + "ms, " + size * 1000L / e + "tps");

		s = System.currentTimeMillis();
		for (int i = 0; i < size; i++) {
			EventKey key = new EventKey("topic", i + "", null);
			map3.remove(key);
		}
		e = System.currentTimeMillis() - s;
		System.out.println("remove :" + e + "ms, " + size * 1000L / e + "tps");

		map3.clear();
	}
}
