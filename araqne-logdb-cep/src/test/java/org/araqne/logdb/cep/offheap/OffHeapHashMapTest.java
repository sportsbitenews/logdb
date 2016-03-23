package org.araqne.logdb.cep.offheap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;

import junit.framework.Assert;

import org.araqne.logdb.cep.offheap.engine.ReferenceStorageEngine;
import org.araqne.logdb.cep.offheap.engine.StorageEngine;
import org.araqne.logdb.cep.offheap.engine.serialize.Serialize;
import org.araqne.logdb.cep.offheap.timeout.OffHeapEventListener;
import org.junit.Test;

public class OffHeapHashMapTest {

	@Test
	public void putTest() {
		OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(
				new ReferenceStorageEngine<String, String>(Serialize.STRING, Serialize.STRING));
		map.put("key", "value");
		assertEquals("value", map.get("key"));
		map.close();
	}

	@Test
	public void updateTest() {
		OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(
				new ReferenceStorageEngine<String, String>(Serialize.STRING, Serialize.STRING));
		map.put("key", "value");
		assertEquals("value", map.get("key"));
		map.put("key", "value2");
		assertEquals("value2", map.get("key"));
		map.close();
	}

	@Test
	public void removeTest() {
		OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(
				new ReferenceStorageEngine<String, String>(Serialize.STRING, Serialize.STRING));
		map.put("key", "value");
		assertEquals("value", map.get("key"));
		map.remove("key");
		assertNull(map.get("key"));
		map.close();
	}

	@Test
	public void getKeysTest() {
		HashSet<String> keys = new HashSet<String>();
		OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(
				new ReferenceStorageEngine<String, String>(Serialize.STRING, Serialize.STRING));
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
	}

	@Test
	public void clearTest() {
		OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(
				new ReferenceStorageEngine<String, String>(Serialize.STRING, Serialize.STRING));
		map.put("key", "value");
		assertEquals("value", map.get("key"));
		map.clear();
		assertNull(map.get("key"));
		// TODO key 갯수 확인

		map.close();
	}

	// @Test
	public void benchTest() {
		OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(
				new ReferenceStorageEngine<String, String>(Serialize.STRING, Serialize.STRING));

		int size = 1000000;
		System.out.println("cnt = " + size + "건");
		long s = System.currentTimeMillis();
		for (int i = 0; i < size; i++) {
			map.put(i + "", "test");
		}
		long e = System.currentTimeMillis() - s;
		System.out.println("put :" + e + "ms, " + size * 1000L / e + "tps");

		s = System.currentTimeMillis();
		for (int i = 0; i < size; i++) {
			// String value = map.get(key.format("%020d", i));
			// System.out.println(i + ": " + map.get(i + ""));
			map.get(i + "");
		}
		e = System.currentTimeMillis() - s;
		System.out.println("get :" + e + "ms, " + size * 1000L / e + "tps");

		s = System.currentTimeMillis();
		for (int i = 0; i < size; i++) {
			// String value = map.get(key.format("%020d", i));
			// System.out.println(i + ": " + map.get(i + ""));
			map.remove(i + "");
		}
		e = System.currentTimeMillis() - s;
		System.out.println("remove :" + e + "ms, " + size * 1000L / e + "tps");

		map.close();
	}

	@Test
	public void putTest2() {
		StorageEngine<People, Pc> storage = new ReferenceStorageEngine<People, Pc>(new People("n"), new Pc("k"));
		OffHeapHashMap<People, Pc> map = new OffHeapHashMap<People, Pc>(storage);

		People kim = new People("kim");
		Pc pc = new Pc("333");
		map.put(kim, pc);
		Pc newPc = map.get(kim);
		assertEquals("333", newPc.getId());
		map.clear();
	}

	@Test
	public void expireTest() throws InterruptedException {
		OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(
				new ReferenceStorageEngine<String, String>(Serialize.STRING, Serialize.STRING));

		map.put("key", "value", null, new Date().getTime(), 0);
		map.setTime(null, new Date().getTime());
		// String value = null;
		// while ((value = map.get("key")) != null) {
		// System.out.println(value);
		// Thread.sleep(1000);
		// }
		// System.out.println("evict!");
		// System.out.println(map.get("key"));
		assertNull(map.get("key"));
		map.close();
	}

	@Test
	public void expireHostTest() throws InterruptedException {
		OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(
				new ReferenceStorageEngine<String, String>(Serialize.STRING, Serialize.STRING));

		map.put("key", "value", "host", new Date().getTime(), 0);
		map.setTime("host", new Date().getTime());
		assertNull(map.get("key"));
		map.close();
	}

	@Test
	public void timeoutTest() throws InterruptedException {
		OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(
				new ReferenceStorageEngine<String, String>(Serialize.STRING, Serialize.STRING));

		map.put("Key", "Value");
		// map.timeout("Key", null, new Date().getTime());
		map.setTime(null, new Date().getTime());
		assertNull(map.get("key"));
		map.close();
	}

	@Test
	public void timeouHosttTest() throws InterruptedException {
		OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(
				new ReferenceStorageEngine<String, String>(Serialize.STRING, Serialize.STRING));

		map.put("Key", "Value");
		// map.timeout("Key", "host", new Date().getTime() + 3000);
		map.setTime("host", new Date().getTime());
		assertNull(map.get("key"));
		map.close();
	}

	@Test
	public void getLastTimeTest() {
		OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(
				new ReferenceStorageEngine<String, String>(Serialize.STRING, Serialize.STRING));
		map.setTime("host", 12345L);
		assertEquals(12345L, map.getLastTime("host"));
		map.close();
	}

	@Test
	public void hostSetTest() {
		OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(
				new ReferenceStorageEngine<String, String>(Serialize.STRING, Serialize.STRING));

		HashSet<String> hosts = new HashSet<String>();
		map.put("key", "value", "host", new Date().getTime() + 3000, 0);
		hosts.add("host");
		map.put("key", "value1", "host1", new Date().getTime() + 3000, 0);
		//hosts.add("host1");
		map.put("key", "value2", "host2", new Date().getTime() + 3000, 0);
		//hosts.add("host2");
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
	public void evnetListnerTest() {
		OffHeapMap<String, String> map = new OffHeapHashMap<String, String>(new ReferenceStorageEngine<String, String>(
				Serialize.STRING, Serialize.STRING));
		OffHeapEventListener<String, String> listener = new OffHeapEventListener<String, String>() {

			@Override
			public void onExpire(String key, String value, long time) {
				System.out.println("on expire callback:[ " + new Date(time) + "] key = " + key + ", value =" + value);
				assertEquals("key", key);
				assertEquals("value", value);
			}
		};

		map.put("key", "value", "host", new Date().getTime() + 0000, 0);
		map.addListener(listener);
		map.setTime("host", new Date().getTime());

		map.close();
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
