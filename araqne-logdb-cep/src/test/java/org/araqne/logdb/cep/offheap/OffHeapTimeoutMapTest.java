package org.araqne.logdb.cep.offheap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;

import junit.framework.Assert;

import org.araqne.logdb.Row;
import org.araqne.logdb.cep.EventContext;
import org.araqne.logdb.cep.EventKey;
import org.araqne.logdb.cep.offheap.engine.ReferenceStorage;
import org.araqne.logdb.cep.offheap.engine.serialize.Serialize;
import org.araqne.logdb.cep.offheap.evict.TimeoutEventListener;
import org.araqne.logdb.cep.offheap.factory.TimeoutMapFactory;
import org.araqne.logdb.cep.offheap.manager.EntryStorageManagerImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OffHeapTimeoutMapTest {

	private EventKey key;
	private EventContext value;

	private People kim;
	private Pc pc;

	private TimeoutMap<String, String> map;
	private TimeoutMap<People, Pc> map2;
	private TimeoutMap<EventKey, EventContext> map3;

	@Before
	public void onStart() {
		map = TimeoutMapFactory.string(1000, 256).map();

		map2 = new OffheapTimeoutMap<People, Pc>(new ReferenceStorage<People, Pc>(1000,
				new EntryStorageManagerImpl<People, Pc>(128, new People("n"), new Pc("k"))));
		kim = new People("kim");
		pc = new Pc("333");

		map3 = TimeoutMapFactory.event(1000, 512).map();
		key = new EventKey("topic", "key", "host");
		value = new EventContext(key, 0L, 0L, 0L, 10);
		Row row1 = new Row();
		row1.put("1", "row");
		value.getCounter().set(12);
		value.addRow(new Row());
		value.addRow(row1);
		value.setVariable("intVar", 1);
		value.setVariable("stringVar", "string");
	}

	@After
	public void onClose() {
		map.close();
		map2.close();
		map3.close();
	}

	@Test
	public void putTest() {
		map.put("key", "value");
		assertEquals("value", map.get("key"));
		map.clear();
	}

	@Test
	public void putTest2() {
		map2.put(kim, pc);
		Pc newPc = map2.get(kim);
		assertEquals("333", newPc.getId());
		map.clear();
	}

	@Test
	public void putTest3() {
		map3.put(key, value);
		assertEquals(value, map3.get(key));
		map3.clear();
	}

	@Test
	public void updateTest() {
		map.put("key", "value");
		assertEquals("value", map.get("key"));
		map.put("key", "value2");
		assertEquals("value2", map.get("key"));
		map.clear();
	}

	@Test
	public void updateTest2() {
		map2.put(kim, pc);
		assertEquals(pc, map2.get(kim));
		Pc newPc = new Pc("1234");
		map2.put(kim, newPc);
		assertEquals(newPc, map2.get(kim));
		map.clear();
	}

	@Test
	public void putIfAbsentTest() {
		map.putIfAbsent("key", "value1", null, 0L, 0L);
		assertEquals("value1", map.get("key"));
		map.putIfAbsent("key", "value2", null, 0L, 0L);
		assertEquals("value1", map.get("key"));
		map.clear();
	}

	@Test
	public void replaceTest() {
		map.putIfAbsent("key", "value1", null, 0L, 0L);
		assertEquals("value1", map.get("key"));
		map.replace("key", "value1", "value2", null, 0L);
		assertEquals("value2", map.get("key"));

		// fail
		map.replace("key", "value1", "value3", null, 0L);
		assertEquals("value2", map.get("key"));
		map.clear();
	}

	@Test
	public void removeTest() {
		map.put("key", "value");
		assertEquals("value", map.get("key"));
		map.remove("key");
		assertNull(map.get("key"));
		map.clear();
	}

	@Test
	public void getKeysTest() {
		HashSet<String> keys = new HashSet<String>();
		map.put("key1", "value1");
		keys.add("key1");
		map.put("key2", "value2");
		keys.add("key2");
		map.put("key3", "value3");
		keys.add("key3");
		map.put("key4", "value4");
		keys.add("key4");
		map.put("key5", "value5");
		keys.add("key5");
		map.put("key6", "value6");
		keys.add("key6");
		map.put("key7", "value7");
		keys.add("key7");
		map.put("key8", "value8");
		keys.add("key8");

		Iterator<String> itr = map.getKeys();
		while (itr.hasNext()) {
			String key = itr.next();
			if (!keys.contains(key)) {
				Assert.fail();
			}
			keys.remove(key);
		}
		assertEquals(0, keys.size());
		map.clear();
	}

	@Test
	public void clearTest() {
		map.put("key", "value");
		assertEquals("value", map.get("key"));
		map.clear();
		assertNull(map.get("key"));
		Iterator<String> keyItr = map.getKeys();
		assertEquals(false, keyItr.hasNext());
	}

	@Test
	public void expireTest() throws InterruptedException {
		map.put("key", "value", null, new Date().getTime(), 0);
		map.setTime(null, new Date().getTime());
		assertNull(map.get("key"));
		map.close();
	}

	@Test
	public void expireHostTest() throws InterruptedException {
		map.put("key", "value", "host", new Date().getTime(), 0);
		map.setTime("host", new Date().getTime());
		assertNull(map.get("key"));
		map.close();
	}

	@Test
	public void timeoutTest() throws InterruptedException {
		map.put("Key", "Value");
		map.setTime(null, new Date().getTime());
		assertNull(map.get("key"));
		map.close();
	}

	@Test
	public void timeouHosttTest() throws InterruptedException {
		map.put("Key", "Value");
		map.setTime("host", new Date().getTime());
		assertNull(map.get("key"));
		map.close();
	}

	@Test
	public void getLastTimeTest() {
		map.setTime("host", 12345L);
		assertEquals(12345L, map.getLastTime("host"));
		map.close();
	}

	@Test
	public void hostSetTest() {
		HashSet<String> hosts = new HashSet<String>();
		map.put("key", "value", "host", new Date().getTime() + 3000, 0);
		hosts.add("host");
		map.put("key", "value1", "host1", new Date().getTime() + 3000, 0);
		// hosts.add("host1");
		map.put("key", "value2", "host2", new Date().getTime() + 3000, 0);
		// hosts.add("host2");
		map.put("Key1", "value");
		map.put("Key1", "value", "host3", 0, new Date().getTime() + 3000);
		hosts.add("host3");
		map.put("Key1", "value", "host4", 0, new Date().getTime() + 3000);
		hosts.add("host4");
		map.put("Key1", "value", "host5", 0, new Date().getTime() + 3000);
		hosts.add("host5");

		assertEquals(hosts, map.hostSet());
		map.close();
	}

	@Test
	public void clearClock() {
		map.put("key1", "value1", null, new Date().getTime(), new Date().getTime());
		assertEquals(true, map.expireQueue(null).contains("value1"));
		assertEquals(true, map.timeoutQueue(null).contains("value1"));
		map.clearClock();
		assertEquals(false, map.expireQueue(null).contains("value1"));
		assertEquals(false, map.timeoutQueue(null).contains("value1"));

		map.put("key2", "value2", null, new Date().getTime(), new Date().getTime());
		assertEquals(true, map.expireQueue(null).contains("value2"));
		assertEquals(true, map.timeoutQueue(null).contains("value2"));
		map.clearClock();
		assertEquals(false, map.expireQueue(null).contains("value2"));
		assertEquals(false, map.timeoutQueue(null).contains("value2"));

		map.put("key2", "value3", null, new Date().getTime(), new Date().getTime());
		assertEquals(true, map.timeoutQueue(null).contains("value3"));
		map.clearClock();
		assertEquals(false, map.timeoutQueue(null).contains("value3"));

		map.remove("key2");
		map.put("key2", "value2", null, new Date().getTime(), new Date().getTime());
		assertEquals(true, map.expireQueue(null).contains("value2"));
		assertEquals(true, map.timeoutQueue(null).contains("value2"));
		map.clearClock();
		assertEquals(false, map.expireQueue(null).contains("value2"));
		assertEquals(false, map.timeoutQueue(null).contains("value2"));

		map.put("key3", "value3", "host", new Date().getTime(), new Date().getTime());
		assertEquals(true, map.expireQueue("host").contains("value3"));
		assertEquals(true, map.timeoutQueue("host").contains("value3"));
		map.clearClock();
		assertEquals(false, map.expireQueue("host").contains("value3"));
		assertEquals(false, map.timeoutQueue("host").contains("value3"));
		map.clear();
	}

	@Test
	public void evnetListnerTest() {
		TimeoutEventListener<String, String> listener = new TimeoutEventListener<String, String>() {

			@Override
			public void onTimeout(String key, String value, long time) {
				// System.out.println("on expire callback:[ " + new Date(time) +
				// "] key = " + key + ", value =" + value);
				assertEquals("key", key);
				assertEquals("value", value);
			}
		};

		map.put("key", "value", "host", new Date().getTime() + 0000, 0);
		map.addListener(listener);
		map.setTime("host", new Date().getTime());

		map.close();
	}

	@Test
	public void closeTest() {
		OffheapTimeoutMap<String, String> map4 = new OffheapTimeoutMap<String, String>(
				new ReferenceStorage<String, String>(1000, new EntryStorageManagerImpl<String, String>(256,
						Serialize.STRING, Serialize.STRING)));

		map4.close();

		try {
			map4.get("a");
		} catch (Exception e) {
			assertEquals("a storage was closed", e.getMessage());
		}

		try {
			map4.put("a", "b");
		} catch (Exception e) {
			assertEquals("a storage was closed", e.getMessage());
		}

		try {
			map4.putIfAbsent("a", "b", null, 0L, 0L);
		} catch (Exception e) {
			assertEquals("a storage was closed", e.getMessage());
		}

		try {
			map4.replace("a", "b", "c", null, 0L);
		} catch (Exception e) {
			assertEquals("a storage was closed", e.getMessage());
		}

		try {
			map4.remove("a");
		} catch (Exception e) {
			assertEquals("a storage was closed", e.getMessage());
		}

		try {
			map4.getKeys();
		} catch (Exception e) {
			assertEquals("a storage was closed", e.getMessage());
		}

		try {
			map4.clear();
		} catch (Exception e) {
			assertEquals("a storage was closed", e.getMessage());
		}

		try {
			map4.close();
		} catch (Exception e) {
			assertEquals("a storage was closed", e.getMessage());
		}

		try {
			map4.setTime(null, 0L);
		} catch (Exception e) {
			assertEquals("a storage was closed", e.getMessage());
		}

		try {
			map4.getLastTime(null);
		} catch (Exception e) {
			assertEquals("a storage was closed", e.getMessage());
		}

		try {
			map4.hostSet();
		} catch (Exception e) {
			assertEquals("a storage was closed", e.getMessage());
		}

		try {
			map4.timeoutQueue(null);
		} catch (Exception e) {
			assertEquals("a storage was closed", e.getMessage());
		}

		try {
			map4.expireQueue(null);
		} catch (Exception e) {
			assertEquals("a storage was closed", e.getMessage());
		}

		try {
			map4.clearClock();
		} catch (Exception e) {
			assertEquals("a storage was closed", e.getMessage());
		}

		TimeoutEventListener<String, String> listener = new TimeoutEventListener<String, String>() {

			@Override
			public void onTimeout(String key, String value, long time) {
				System.out.println("on expire callback:[ " + new Date(time) + "] key = " + key + ", value =" + value);
				assertEquals("key", key);
				assertEquals("value", value);
			}
		};

		try {
			map4.addListener(listener);
		} catch (Exception e) {
			assertEquals("a storage was closed", e.getMessage());
		}

		try {
			map4.removeListener(listener);
		} catch (Exception e) {
			assertEquals("a storage was closed", e.getMessage());
		}

	}

	private class People implements Serialize<People> {
		public String name;

		public People(String name) {
			this.name = name;
		}

		@Override
		public String toString() {
			return "name : " + name;
		}

		@Override
		public int size() {
			return -1;
		}

		@Override
		public byte[] serialize(People value) {
			return value.name.getBytes();
		}

		@Override
		public People deserialize(byte[] in) {
			return new People(new String(in));
		}

		@Override
		public boolean equals(Object o) {
			return name.equals(((People) o).name);
		}
	}

	private class Pc implements Serialize<Pc> {
		public String id;

		public String getId() {
			return id;
		}

		public Pc(String id) {
			this.id = id;
		};

		@Override
		public boolean equals(Object o) {
			return id.equals(((Pc) o).id);
		}

		@Override
		public String toString() {
			return "id : " + id;
		}

		@Override
		public int size() {
			return -1;
		}

		@Override
		public byte[] serialize(Pc value) {
			return value.id.getBytes();
		}

		@Override
		public Pc deserialize(byte[] in) {
			return new Pc(new String(in));
		}
	}

}
