package org.araqne.logdb.cep.offheap.engine;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

import org.araqne.logdb.cep.offheap.engine.serialize.Serialize;
import org.araqne.logdb.cep.offheap.timeout.TimeoutItem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ReferenceStorageEngineTest {

	private ReferenceStorageEngine<String, String> engine;

	@Before
	public void onStart() {
		engine = new ReferenceStorageEngine<String, String>(128, 128, Serialize.STRING, Serialize.STRING); // *
	}

	@After
	public void onClose() throws IOException {
		engine.close();
	}

	/*
	 * test method : add(), getValue(), getEntry();
	 */
	@Test
	public void getValueTest() {
		String key = "ADD_TEST_KEY";
		String value = "ADD_TEST_VALUE";
		int hash = key.hashCode();
		long time = new Date().getTime();

		long address = engine.add(hash, key, value, time);
		assertEquals(value, engine.getValue(address));

		Entry<String, String> entry = engine.getEntry(address);
		assertEquals(key, entry.getKey());
		assertEquals(value, entry.getValue());
		assertEquals(hash, entry.getHash());
		assertEquals(0, entry.getNext());
		assertEquals(time, entry.getTimeoutTime());

		engine.remove(hash, key);

		String key2 = "ADD_TEST_KEY_2";
		long address2 = engine.add(hash, key2, value, time);

		String key3 = "ADD_TEST_KEY_3";
		long address3 = engine.add(hash, key3, value, time);

		entry = engine.getEntry(address3);
		assertEquals(key3, entry.getKey());
		assertEquals(address2, entry.getNext());

		engine.remove(hash, key2);
		engine.remove(hash, key3);
	}

	/*
	 * test method : findAddress
	 */
	@Test
	public void findAddressTest() {
		String key = "FINDADDRESS_TEST_KEY";
		String value = "FINDADDRESS_TEST_VALUE";
		int hash = key.hashCode();

		long address1 = engine.add(hash, key, value);
		long address = engine.findAddress(hash, key);

		assertEquals(address1, address);
		engine.remove(hash, key);
	}

	/*
	 * test method : remove()
	 */
	@Test
	public void removeTest() {
		String key = "REMOVE_TEST_KEY";
		String value = "REMOVE_TEST_VALUE";
		int hash = key.hashCode();

		engine.add(hash, key, value);
		assertEquals(true, engine.remove(hash, key));
		assertEquals(0L, engine.findAddress(hash, key));

		long time = new Date().getTime();

		String key2 = "REMOVE_TEST_KEY_2";
		long address2 = engine.add(hash, key2, value, time);
		String key3 = "REMOVE_TEST_KEY_3";
		long address3 = engine.add(hash, key3, value, time);

		Entry<String, String> entry = engine.getEntry(address3);
		assertEquals(address2, entry.getNext());

		assertEquals(true, engine.remove(hash, key2));
		assertEquals(0L, engine.findAddress(hash, key2));

		entry = engine.getEntry(address3);
		assertEquals(0L, entry.getNext());
		assertEquals(true, engine.remove(hash, key3));
		assertEquals(0L, engine.findAddress(hash, key3));
	}

	/*
	 * test method : replace()
	 */
	@Test
	public void replaceTest() {
		String key = "REPLACE_TEST_KEY";
		String value = "REPLACE_TEST_VALUE";
		int hash = key.hashCode();
		long time = 0L;

		long address = engine.add(hash, key, value, time);

		String newValue = "REPLACE_NEW_TEST_VALUE";
		long newTime = new Date().getTime();
		long newAddress = engine.replace(hash, address, newValue, newTime);

		Entry<String, String> entry = engine.getEntry(newAddress);
		assertEquals(newValue, entry.getValue());
		assertEquals(newTime, entry.getTimeoutTime());

		String newLongValue = "REPLACE_NEW_LONG_TEST_VALUE_REPLACE_NEW_LONG_TEST_VALUE_REPLACE_NEW_LONG_TEST_VALUE_REPLACE_NEW_LONG_TEST_VALUE";
		long newTime2 = new Date().getTime() + 1234;
		long newAddress2 = engine.replace(hash, address, newLongValue, newTime2);

		entry = engine.getEntry(newAddress2);
		assertEquals(newLongValue, entry.getValue());
		assertEquals(newTime2, entry.getTimeoutTime());
		assertEquals(256, entry.getMaxSize());
		assertEquals(true, engine.remove(hash, key));
	}

	/*
	 * test method : evict()
	 */
	@Test
	public void evictTest() {
		String key = "EVICT_TEST_KEY";
		String value = "EVICT_TEST_VALUE";
		int hash = key.hashCode();
		long time = new Date().getTime();

		long address = engine.add(hash, key, value, time);
		assertEquals(address, engine.findAddress(hash, key));
		engine.evict(new TimeoutItem(time, address));
		assertEquals(0, engine.findAddress(hash, key));
	}

	/*
	 * test method : getKeys()
	 */
	@Test
	public void keySetTest() {
		String[] keys = { "0", "1", "2", "3", "4", "5", "6" };

		engine.add(0, keys[0], "");
		engine.add(1, keys[3], "a");
		engine.add(1, keys[2], "b");
		engine.add(1, keys[1], "c");
		engine.add(4, keys[5], "d");
		engine.add(4, keys[4], "e");
		engine.add(5, keys[6], "f");

		Iterator<String> keyItr = engine.getKeys();
		int i = 0;
		while (keyItr.hasNext()) {
			String key = keyItr.next();
			assertEquals(keys[i++], key);
		}

		engine.remove(0, keys[0]);
		engine.remove(1, keys[3]);
		engine.remove(1, keys[2]);
		engine.remove(1, keys[1]);
		engine.remove(4, keys[5]);
		engine.remove(4, keys[4]);
		engine.remove(5, keys[6]);
	}

	// // 3
	// engine.add(100003, "3", "c");
	// engine.add(100003, "2", "b");
	// engine.add(100003, "1", "a");
	//
	// entry1 = engine.get(100003);
	// assertEquals("1", entry1.getKey());
	// assertEquals("a", entry1.getValue());
	//
	// entry2 = engine.getNext(entry1);
	// assertEquals("2", entry2.getKey());
	// assertEquals("b", entry2.getValue());
	//
	// Entry<String, String> entry3 = engine.getNext(entry2);
	// assertEquals("3", entry3.getKey());
	// assertEquals("c", entry3.getValue());
	//
	// engine.remove(entry2, entry1);
	// entry1 = engine.get(100003);
	// assertEquals("1", entry1.getKey());
	// assertEquals("a", entry1.getValue());
	//
	// entry2 = engine.getNext(entry1);
	// assertEquals("3", entry2.getKey());
	// assertEquals("c", entry2.getValue());
	//
	// entry3 = engine.getNext(entry2);
	// assertNull(entry3);
	// }
	//
	// @Test
	// public void updateTest() {
	// engine.add(999, "abcd", "1234");
	// Entry<String, String> entry = engine.get(999);
	// assertEquals("abcd", entry.getKey());
	// assertEquals("1234", entry.getValue());
	//
	// engine.update(entry, null, "5678");
	//
	// entry = engine.get(999);
	// assertEquals("abcd", entry.getKey());
	// assertEquals("5678", entry.getValue());
	// }
	//
	// @Test
	// public void clearTest() {
	// engine.add(111, "!", "1");
	// Entry<String, String> entry = engine.get(111);
	// assertEquals("!", entry.getKey());
	// assertEquals("1", entry.getValue());
	//
	// engine.clear();
	// entry = engine.get(111);
	// assertNull(entry);
	// }
	//
	//
	// @Test
	// public void loadTest() {
	// int index = 125491;
	//
	// long address = engine.add(index, "key", "value");
	// Entry<String, String> e1 = engine.get(index);
	// Entry<String, String> e2 = engine.getEntry(address);
	// String key = engine.loadKey(address);
	// String value = engine.loadValue(address);
	//
	// assertEquals("key", e1.getKey());
	// assertEquals("key", e2.getKey());
	// assertEquals("key", key);
	//
	// assertEquals("value", e1.getValue());
	// assertEquals("value", e2.getValue());
	// assertEquals("value", value);
	// }
	//
	// @Test
	// public void addExpireTest() {
	// long timeout = new Date().getTime();
	//
	// long address = engine.add(62415, "Key", "Value", timeout);
	// Entry<String, String> entry = engine.get(62415);
	// assertEquals(timeout, entry.getTimeoutTime());
	//
	// engine.evict(new TimeoutItem(timeout, address));
	// Entry<String, String> entry2 = engine.get(62415);
	// assertNull(entry2);
	// }

}
