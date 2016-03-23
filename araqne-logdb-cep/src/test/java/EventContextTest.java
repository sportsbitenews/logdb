import static org.junit.Assert.assertEquals;

import java.util.Date;

import org.araqne.logdb.Row;
import org.araqne.logdb.cep.EventContext;
import org.araqne.logdb.cep.EventKey;
import org.araqne.logdb.cep.offheap.ConcurrentOffHeapHashMap;
import org.araqne.logdb.cep.offheap.engine.ReferenceStorageEngineFactory;
import org.araqne.logdb.cep.offheap.engine.StorageEngineFactory;
import org.araqne.logdb.cep.offheap.engine.serialize.EventContextSerialize;
import org.araqne.logdb.cep.offheap.engine.serialize.EventKeySerialize;
import org.araqne.logdb.cep.offheap.engine.serialize.Serialize;
import org.junit.Test;

public class EventContextTest {
	//
	// @Test
	// public void cloneTest() {
	// EventKey key = new EventKey("topic", "key", "host");
	// EventContext value = new EventContext(key, 0L, 0L, 0L, 10);
	// Row row1 = new Row();
	// row1.put("1", "row");
	// value.getCounter().set(12);
	// value.addRow(new Row());
	// value.addRow(row1);
	// value.setVariable("intVar", 1);
	// value.setVariable("stringVar", "string");
	//
	// EventContext cloned = value.clone();
	//
	// System.out.println(cloned.getRows());
	// }

	@Test
	public void test() {
		EventKey key = new EventKey("topic", "key", "host");
		Serialize<EventKey> keySerializer = new EventKeySerialize();

		byte[] b = null;
		int size = 100000;
		long s = System.currentTimeMillis();
		for (int i = 0; i < size; i++) {
			b = keySerializer.serialize(key);
		}

		long e = System.currentTimeMillis() - s;
		System.out.println(e + " " + size * 1000L / e);

		s = System.currentTimeMillis();
		for (int i = 0; i < size; i++) {
			key = keySerializer.deserialize(b);
		}

		e = System.currentTimeMillis() - s;
		System.out.println(e + " " + size * 1000L / e);
		System.out.println(key);
	}

	@Test
	public void equalTest() {
		EventKey key = new EventKey("topic", "key", "host");
		EventContext value = new EventContext(key, 0L, 0L, 0L, 10);
		Row row1 = new Row();
		row1.put("1", "row");
		value.getCounter().set(12);
		value.addRow(new Row());
		value.addRow(row1);
		value.setVariable("intVar", 1);
		value.setVariable("stringVar", "string");

		EventContext cloned = value.clone();
		assertEquals(value, cloned);
	}

	@Test
	public void encodingBenchTest() {
		 Serialize<EventKey> keySerializer = new EventKeySerialize();
		 Serialize<EventContext> valueSerializer = new EventContextSerialize();
		EventKey key = new EventKey("demo", "999999", null);
		EventContext value = new EventContext(key, new Date().getTime(), 0L, 0L, 10);
		value.getCounter().set(1);
		Row row1 = new Row();
		row1.put("_table", "t");
		row1.put("_id", 999999);
		row1.put("_time", "2016-03-10 19:47:07+0900");
		row1.put("key", "999999");
		//value.addRow(row1);
		//value.setVariable("intVar", 1);
		//value.setVariable("stringVar", "string");

		int size = 1000000;
		long s = System.currentTimeMillis();
		for (int i = 0; i < size; i++) {
			byte[] k = keySerializer.serialize(key);
			byte[] v =valueSerializer.serialize(value);
			
			keySerializer.deserialize(k);
			valueSerializer.deserialize(v);
		}
		
		long e = System.currentTimeMillis() - s;
		System.out.println(e + " " + size * 1000L / e);
	}

	
	//@Test
	public void putBenchTest() {

		ConcurrentOffHeapHashMap<EventKey, EventContext> map;
		StorageEngineFactory<EventKey, EventContext> factory = new ReferenceStorageEngineFactory<EventKey, EventContext>(
				1024 * 1024 * 16, 512, new EventKeySerialize(), new EventContextSerialize());
		map = new ConcurrentOffHeapHashMap<EventKey, EventContext>(factory);
		
		// ConcurrentHashMap<Integer, byte[]> map = new
		// ConcurrentHashMap<Integer, byte[]>();
		// Serialize<EventKey> keySerializer = new EventKeySerialize();
		// Serialize<EventContext> valueSerializer = new
		// EventContextSerialize();
		// byte[] keyBytes = keySerializer.serialize(key);
		// byte[] valueBytes = valueSerializer.serialize(value);
		//
		// Entry<byte[], byte[]> entry = new Entry<byte[], byte[]>(keyBytes,
		// valueBytes, 0, 0, 0, 0);
		// Entry<EventKey, EventContext> entry = new Entry<EventKey,
		// EventContext>(key, value, 10, 12415L, 1254125125L,
		// 1231231523L);
		EventKey key = new EventKey("demo", "999999", null);
		EventContext value = new EventContext(key, new Date().getTime(), 0L, 0L, 1);
		value.getCounter().set(1);
		Row row1 = new Row();
		row1.put("_table", "t");
		row1.put("_id", 999999);
		row1.put("_time", "2016-03-10 19:47:07+0900");
		row1.put("key", "999999");
		value.addRow(row1);
		//value.setVariable("intVar", 1);
		//value.setVariable("stringVar", "string");
		

		int size = 1000000;
		long s = System.currentTimeMillis();
		for (int i = 0; i < size; i++) {
			map.put(key, value);
		}
		long e = System.currentTimeMillis() - s;
		System.out.println(e + " " + size * 1000L / e);
		map.close();
	}

	// @Test
	// public void encodeBenchTest() {
	// EventKey key = new EventKey("topic", "key", "host");
	// EventContext value = new EventContext(key, 0L, 0L, 0L, 10);
	// Row row1 = new Row();
	// row1.put("1", "row");
	// value.getCounter().set(12);
	// value.addRow(new Row());
	// value.addRow(row1);
	// value.setVariable("intVar", 1);
	// value.setVariable("stringVar", "string");
	//
	// Serialize<EventKey> keySerializer = new EventKeySerialize();
	// Serialize<EventContext> valueSerializer = new EventContextSerialize();
	//
	// int size = 100000;
	// long s = System.currentTimeMillis();
	// for (int i = 0; i < size; i++) {
	// Entry<EventKey, EventContext> entry = new Entry<EventKey,
	// EventContext>(key, value, 124, 1354L, 12515L);
	// byte[] b = Entry.encode(entry, keySerializer, valueSerializer);
	// Entry.decode(b, keySerializer, valueSerializer, 1254L);
	// }
	// long e = System.currentTimeMillis() - s;
	// System.out.println(e + " " + size * 1000L / e);
	//
	// }
	//
	// @Test
	// public void encodeDecodeTest() {
	// EventKey key = new EventKey("topic", "key", "host");
	// EventContext value = new EventContext(key, 0L, 0L, 0L, 10);
	// Row row1 = new Row();
	// row1.put("1", "row");
	// value.getCounter().set(12);
	// value.addRow(new Row());
	// value.addRow(row1);
	// value.setVariable("intVar", 1);
	// value.setVariable("stringVar", "string");
	//
	// Serialize<EventKey> keySerializer = new EventKeySerialize();
	// Serialize<EventContext> valueSerializer = new EventContextSerialize();
	//
	// Entry<EventKey, EventContext> entry = new Entry<EventKey,
	// EventContext>(key, value, 124, 1354L, 12515L, 125L);
	// byte[] b = Entry.encode(entry, keySerializer, valueSerializer);
	// Entry<EventKey, EventContext> entry2 = Entry.decode(b, keySerializer,
	// valueSerializer, 1254L);
	//
	// System.out.println(entry2.getKey());
	// System.out.println(entry2.getValue().getVariables());
	//
	// }
	//
	// //@Test
	// public void encodeDecodeTest2() {
	// EventKey key = new EventKey("topic", "key", "host");
	// EventContext value = new EventContext(key, 0L, 0L, 0L, 10);
	// Row row1 = new Row();
	// row1.put("1", "row");
	// value.getCounter().set(12);
	// value.addRow(new Row());
	// value.addRow(row1);
	// value.setVariable("intVar", 1);
	// value.setVariable("stringVar", "string");
	//
	// Serialize<EventKey> keySerializer = new EventKeySerialize();
	// Serialize<EventContext> valueSerializer = new EventContextSerialize();
	//
	// int size = 10000;
	// long s = System.currentTimeMillis();
	// for (int i = 0; i < size; i++) {
	// ByteBuffer keybb = keySerializer.serialize(key);
	// EventKey key2 = keySerializer.deserialize(keybb);
	//
	// ByteBuffer bb = valueSerializer.serialize(value);
	// EventContext value2 = valueSerializer.deserialize(bb);
	// }
	// long e = System.currentTimeMillis() - s;
	// System.out.println(e + " " + size * 1000L / e);
	//
	// // System.out.println(key2);
	// // System.out.println(value2.getKey());
	// // System.out.println(value2.getVariables());
	//
	// }
}
