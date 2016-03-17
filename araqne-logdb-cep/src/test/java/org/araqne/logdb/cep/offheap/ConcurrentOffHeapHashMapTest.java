package org.araqne.logdb.cep.offheap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;

import junit.framework.Assert;

import org.araqne.logdb.cep.EventContext;
import org.araqne.logdb.cep.EventKey;
import org.araqne.logdb.cep.offheap.engine.Entry;
import org.araqne.logdb.cep.offheap.engine.ReferenceStorageEngine;
import org.araqne.logdb.cep.offheap.engine.ReferenceStorageEngineFactory;
import org.araqne.logdb.cep.offheap.engine.StorageEngine;
import org.araqne.logdb.cep.offheap.engine.StorageEngineFactory;
import org.araqne.logdb.cep.offheap.engine.serialize.EventContextSerialize;
import org.araqne.logdb.cep.offheap.engine.serialize.EventKeySerialize;
import org.araqne.logdb.cep.offheap.engine.serialize.Serialize;
import org.araqne.logdb.cep.offheap.ConcurrentOffHeapHashMap;
import org.junit.Test;

public class ConcurrentOffHeapHashMapTest {

	//@Test
	public void evnetTest2() {
		StorageEngineFactory<EventKey, EventContext> factory = new ReferenceStorageEngineFactory<EventKey, EventContext>(
				512, new EventKeySerialize(), new EventContextSerialize());
		OffHeapConcurrency<EventKey, EventContext> map = new ConcurrentOffHeapHashMap<EventKey, EventContext>(factory);
		// Serialize<EventContext> valueSerializer = new
		// EventContextSerialize();
		// Serialize<EventKey> keySerializer = new EventKeySerialize();

		for (int i = 0; i < 100000; i++) {
			EventKey key = new EventKey("topic" + i, "key", "host");
			EventContext value = new EventContext(key, 0L, 0L, 0L, 10);
			map.put(key, value, "test", 1000);
		}

		long s = System.currentTimeMillis();
		Iterator<EventKey> keys = map.getKeys();
		int i = 0;
		while (keys.hasNext()) {
			EventKey k = keys.next();
			// System.out.println(++i + " " +k + "  " + map.get(k));
			i++;
		}
		System.out.println(i + " " + (System.currentTimeMillis() - s));

		map.close();
	}

	//@Test
	public void entryEncodeTest() {
		long s = System.currentTimeMillis();
		int size = 1000000;
		for (int i = 0; i < size; i++) {
			Entry<String, String> e = new Entry<String, String>("a", "b", 0, 0, 0, 0);
			//Entry.lengthOf(Entry.marshal(e,  Serialize.STRING, Serialize.STRING));
			//Entry.sizeOf(keyLength, valueLength)
			Entry.decode(Entry.encode(e, Serialize.STRING, Serialize.STRING), Serialize.STRING, Serialize.STRING, 0L);
			//Entry.decodeEntry(Entry.encodeEntry(e, Serialize.STRING, Serialize.STRING), Serialize.STRING,
				//	Serialize.STRING, 0L);
		}
		
		long e = System.currentTimeMillis() - s;
		System.out.println("put :" + e + "ms, " + size * 1000L / e + "tps");
	}

	//@Test
	public void evnetTest() {
		StorageEngine<EventKey, EventContext> engine = new ReferenceStorageEngine<EventKey, EventContext>(100,
				new EventKeySerialize(), new EventContextSerialize());
		StorageEngineFactory<EventKey, EventContext> factory = new ReferenceStorageEngineFactory<EventKey, EventContext>(
				new EventKeySerialize(), new EventContextSerialize());
		OffHeapConcurrency<EventKey, EventContext> map = new ConcurrentOffHeapHashMap<EventKey, EventContext>(factory);

		EventKey key = new EventKey("topic", "key", "host");
		EventContext value = new EventContext(key, 0L, 0L, 0L, 10);

		EventContext clone = value.clone();

		Serialize<EventContext> valueSerializer = new EventContextSerialize();
		Serialize<EventKey> keySerializer = new EventKeySerialize();

//		byte[] b = valueSerializer.serialize(value).array();
//		System.out.println(Arrays.toString(b));
//		EventContext c = valueSerializer.deserialize(b, 0, b.length);
//		System.out.println(c.getKey() + " " + c.getExpireTime());
//
//		byte[] b2 = keySerializer.serialize(key).array();
//		System.out.println(keySerializer.deserialize(b2, 0, b2.length));
//
//		map.put(key, value);
//		EventContext value2 = new EventContext(key, 0L, 0L, 0L, 20);
//
//		map.replace(key, clone, value2);
//
//		EventContext e = map.get(key);
//		System.out.println(e.getKey() + " " + e.getExpireTime());

		// map.put("key", "value");
		// assertEquals("value", map.get("key"));
		map.close();
	}

	//@Test
	public void benchTest() {
		// StorageEngineFactory<String, String> factory = new
		// ReferenceStorageEngineFactory<String, String>(
		// Serialize.STRING, Serialize.STRING);
		// OffHeapConcurrency<String, String> map = new
		// ConcurrentOffHeapHashMap<String, String>(factory);
		//
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
	public void putTest() {
		StorageEngineFactory<String, String> factory = new ReferenceStorageEngineFactory<String, String>(
				1000, Serialize.STRING, Serialize.STRING);
		OffHeapConcurrency<String, String> map = new ConcurrentOffHeapHashMap<String, String>(7, factory);

		map.put("putKey", "putValue");
		assertEquals("putValue", map.get("putKey"));
		map.close();
	}

	@Test
	public void updateTest() {
		StorageEngineFactory<String, String> factory = new ReferenceStorageEngineFactory<String, String>(
				1000, Serialize.STRING, Serialize.STRING);
		OffHeapConcurrency<String, String> map = new ConcurrentOffHeapHashMap<String, String>(7, factory);

		map.put("key", "value");
		assertEquals("value", map.get("key"));
		map.put("key", "value2");
		assertEquals("value2", map.get("key"));
		map.close();
	}

	@Test
	public void removeTest() {
		StorageEngineFactory<String, String> factory = new ReferenceStorageEngineFactory<String, String>(
				1000, Serialize.STRING, Serialize.STRING);
		OffHeapConcurrency<String, String> map = new ConcurrentOffHeapHashMap<String, String>(7, factory);

		map.put("key", "value");
		assertEquals("value", map.get("key"));
		map.remove("key");
		assertNull(map.get("key"));
		map.close();
	}

	@Test
	public void getKeysTest() {
		HashSet<String> keys = new HashSet<String>();
		StorageEngineFactory<String, String> factory = new ReferenceStorageEngineFactory<String, String>(1000,
			  Serialize.STRING, Serialize.STRING);
		OffHeapConcurrency<String, String> map = new ConcurrentOffHeapHashMap<String, String>(7, factory);

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
			System.out.println(key);
			if (!keys.contains(key)) {
				Assert.fail();
			}
			keys.remove(key);
		}
		assertEquals(0, keys.size());
	}

	// @Test
	// public void clearTest() {
	// OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(
	// new ReferenceStorageEngine<String, String>(Serialize.STRING,
	// Serialize.STRING));
	// map.put("key", "value");
	// assertEquals("value", map.get("key"));
	// map.clear();
	// assertNull(map.get("key"));
	// // TODO key 갯수 확인
	//
	// map.close();
	// }
	//
	// // @Test
	// public void benchTest() {
	// OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(
	// new ReferenceStorageEngine<String, String>(Serialize.STRING,
	// Serialize.STRING));
	//
	// int size = 1000000;
	// System.out.println("cnt = " + size + "건");
	// long s = System.currentTimeMillis();
	// for (int i = 0; i < size; i++) {
	// map.put(i + "", "test");
	// }
	// long e = System.currentTimeMillis() - s;
	// System.out.println("put :" + e + "ms, " + size * 1000L / e + "tps");
	//
	// s = System.currentTimeMillis();
	// for (int i = 0; i < size; i++) {
	// // String value = map.get(key.format("%020d", i));
	// // System.out.println(i + ": " + map.get(i + ""));
	// map.get(i + "");
	// }
	// e = System.currentTimeMillis() - s;
	// System.out.println("get :" + e + "ms, " + size * 1000L / e + "tps");
	//
	// s = System.currentTimeMillis();
	// for (int i = 0; i < size; i++) {
	// // String value = map.get(key.format("%020d", i));
	// // System.out.println(i + ": " + map.get(i + ""));
	// map.remove(i + "");
	// }
	// e = System.currentTimeMillis() - s;
	// System.out.println("remove :" + e + "ms, " + size * 1000L / e + "tps");
	//
	// map.close();
	// }
	//
	// @Test
	// public void putTest2() {
	// StorageEngine<People, Pc> storage = new ReferenceStorageEngine<People,
	// Pc>(new People("n"), new Pc("k"));
	// OffHeapHashMap<People, Pc> map = new OffHeapHashMap<People, Pc>(storage);
	//
	// People kim = new People("kim");
	// Pc pc = new Pc("333");
	// map.put(kim, pc);
	// Pc newPc = map.get(kim);
	// assertEquals("333", newPc.getId());
	// map.clear();
	// }
	//
	// @Test
	// public void expireTest() throws InterruptedException {
	// OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(
	// new ReferenceStorageEngine<String, String>(Serialize.STRING,
	// Serialize.STRING));
	//
	// map.put("key", "value", null, new Date().getTime());
	// map.setTime(null, new Date().getTime());
	// // String value = null;
	// // while ((value = map.get("key")) != null) {
	// // System.out.println(value);
	// // Thread.sleep(1000);
	// // }
	// // System.out.println("evict!");
	// // System.out.println(map.get("key"));
	// assertNull(map.get("key"));
	// map.close();
	// }
	//
	// @Test
	// public void expireHostTest() throws InterruptedException {
	// OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(
	// new ReferenceStorageEngine<String, String>(Serialize.STRING,
	// Serialize.STRING));
	//
	// map.put("key", "value", "host", new Date().getTime());
	// map.setTime("host", new Date().getTime());
	// assertNull(map.get("key"));
	// map.close();
	// }
	//
	// @Test
	// public void timeoutTest() throws InterruptedException {
	// OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(
	// new ReferenceStorageEngine<String, String>(Serialize.STRING,
	// Serialize.STRING));
	//
	// map.put("Key", "Value");
	// map.timeout("Key", null, new Date().getTime());
	// map.setTime(null, new Date().getTime());
	// assertNull(map.get("key"));
	// map.close();
	// }
	//
	// @Test
	// public void timeouHosttTest() throws InterruptedException {
	// OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(
	// new ReferenceStorageEngine<String, String>(Serialize.STRING,
	// Serialize.STRING));
	//
	// map.put("Key", "Value");
	// map.timeout("Key", "host", new Date().getTime() + 3000);
	// map.setTime("host", new Date().getTime());
	// assertNull(map.get("key"));
	// map.close();
	// }
	//
	// @Test
	// public void getLastTimeTest() {
	// OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(
	// new ReferenceStorageEngine<String, String>(Serialize.STRING,
	// Serialize.STRING));
	// map.setTime("host", 12345L);
	// assertEquals(12345L, map.getLastTime("host"));
	// map.close();
	// }
	//
	// @Test
	// public void hostSetTest() {
	// OffHeapHashMap<String, String> map = new OffHeapHashMap<String, String>(
	// new ReferenceStorageEngine<String, String>(Serialize.STRING,
	// Serialize.STRING));
	// map.put("key", "value", "host", new Date().getTime() + 3000);
	// map.put("key", "value", "host1", new Date().getTime() + 3000);
	// map.put("key", "value", "host2", new Date().getTime() + 3000);
	// map.put("Key1", "value");
	// map.timeout("Key1", "host3", new Date().getTime() + 3000);
	// map.timeout("Key1", "host4", new Date().getTime() + 3000);
	// map.timeout("Key1", "host5", new Date().getTime() + 3000);
	// System.out.println(map.hostSet());
	// map.close();
	// }
	//
	// @Test
	// public void evnetListnerTest() {
	// OffHeapMap<String, String> map = new OffHeapHashMap<String, String>(new
	// ReferenceStorageEngine<String, String>(
	// Serialize.STRING, Serialize.STRING));
	//
	// OffHeapEventListener listener = new OffHeapEventListener() {
	//
	// @Override
	// public void onExpire() {
	// System.out.println("evict");
	// }
	// };
	//
	// map.put("key", "value", "host", new Date().getTime() + 0000);
	// map.addListener(listener);
	// map.setTime("host", new Date().getTime());
	//
	// map.close();
	// }

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

//		@Override
//		public byte[] serialize(People value) {
//			return value.name.getBytes();
//		}
//
//		@Override
//		public People deserialize(byte[] in, int s, int e) {
//			return new People(new String(in, s, e));
//		}

		@Override
		public boolean equals(Object o) {
			return name.equals(((People) o).name);
		}

		@Override
		public People deserialize(ByteBuffer bb) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ByteBuffer serialize(People value) {
			// TODO Auto-generated method stub
			return null;
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

//		@Override
//		public byte[] serialize(Pc value) {
//			return value.id.getBytes();
//		}
//
//		@Override
//		public Pc deserialize(byte[] in, int s, int e) {
//			return new Pc(new String(in, s, e));
//		}

		@Override
		public Pc deserialize(ByteBuffer bb) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public ByteBuffer serialize(Pc value) {
			// TODO Auto-generated method stub
			return null;
		}
	}

}
