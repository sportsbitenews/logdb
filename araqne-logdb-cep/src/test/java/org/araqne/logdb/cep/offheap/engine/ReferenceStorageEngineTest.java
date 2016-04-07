package org.araqne.logdb.cep.offheap.engine;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;

import org.araqne.logdb.cep.offheap.engine.serialize.Serialize;
import org.araqne.logdb.cep.offheap.evict.EvictItem;
import org.araqne.logdb.cep.offheap.manager.EntryStorageManager;
import org.araqne.logdb.cep.offheap.manager.EntryStorageManagerImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ReferenceStorageEngineTest {

	private ReferenceStorage<String, String> engine;

	@Before
	public void onStart() {
		EntryStorageManager<String, String> manager = new EntryStorageManagerImpl<String, String>(256,
				Serialize.STRING, Serialize.STRING);
		engine = new ReferenceStorage<String, String>(128, manager);
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
		long time = new Date().getTime();

		long address = engine.add(key, value, time);
		assertEquals(value, engine.getValue(address));

		Entry<String, String> entry = engine.getEntry(address);
		assertEquals(key, entry.getKey());
		assertEquals(value, entry.getValue());
		assertEquals(0, entry.getNext());
		assertEquals(time, entry.getEvictTime());

		engine.remove(key);

		String key2 = "ADD_TEST_KEY_2";
		long address2 = engine.add(key2, value, time);

		String key3 = "ADD_TEST_KEY_3";
		long address3 = engine.add(key3, value, time);

		entry = engine.getEntry(address2);
		assertEquals(key2, entry.getKey());
		assertEquals(value, entry.getValue());
		
		entry = engine.getEntry(address3);
		assertEquals(key3, entry.getKey());
		assertEquals(value, entry.getValue());
		
		assertEquals(true, engine.remove(key2));
		assertEquals(true, engine.remove(key3));
	}

	/*
	 * test method : findAddress
	 */
	@Test
	public void findAddressTest() {
		String key = "FINDADDRESS_TEST_KEY";
		String value = "FINDADDRESS_TEST_VALUE";

		long address1 = engine.add(key, value);
		long address = engine.findAddress(key);

		assertEquals(address1, address);
		assertEquals(true, engine.remove(key));
	}

	/*
	 * test method : remove()
	 */
	@Test
	public void removeTest() {
		String key = "REMOVE_TEST_KEY";
		String value = "REMOVE_TEST_VALUE";

		engine.add(key, value);
		assertEquals(true, engine.remove(key));
		assertEquals(0L, engine.findAddress(key));

		long time = new Date().getTime();

		String key2 = "REMOVE_TEST_KEY_2";
		long address2 = engine.add(key2, value, time);
		String key3 = "REMOVE_TEST_KEY_3";
		long address3 = engine.add(key3, value, time);

		Entry<String, String> entry = engine.getEntry(address3);
		assertEquals(key3, entry.getKey());

		assertEquals(true, engine.remove(key2));
		assertEquals(0L, engine.findAddress(key2));

		entry = engine.getEntry(address3);
		assertEquals(0L, entry.getNext());
		assertEquals(true, engine.remove(key3));
		assertEquals(0L, engine.findAddress(key3));
	}

	/*
	 * test method : updateValue()
	 */
	@Test
	public void updateValueTest() {
		String key = "UPDATE_VALUE_TEST_KEY";
		String value = "UPDATE_VALUE_TEST_VALUE";
		int hash = key.hashCode();
		long time = 0L;

		long address = engine.add(key, value, time);

		String newValue = "UPDATE_VALUE_NEW_TEST_VALUE";
		long newTime = new Date().getTime();
		long newAddress = engine.updateValue(address, key, newValue, newTime);

		Entry<String, String> entry = engine.getEntry(newAddress);
		assertEquals(newValue, entry.getValue());
		assertEquals(newTime, entry.getEvictTime());

		String newLongValue = "UPDATE_VALUE_NEW_LONG_TEST_UPDATE_VALUE_NEW_LONG_TEST_UPDATE_VALUE_NEW_LONG_TEST_UPDATE_VALUE_NEW_LONG_TEST";
		long newTime2 = new Date().getTime() + 1234;
		long newAddress2 = engine.updateValue(address, key, newLongValue, newTime2);

		entry = engine.getEntry(newAddress2);
		assertEquals(newLongValue, entry.getValue());
		assertEquals(newTime2, entry.getEvictTime());
		assertEquals(256, entry.getMaxSize());
		assertEquals(true, engine.remove(key));
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

		long address = engine.add(key, value, time);

		String newValue = "REPLACE_NEW_TEST_VALUE";
		long newTime = new Date().getTime();
		long newAddress = engine.replace(address, key, value, newValue, newTime);

		Entry<String, String> entry = engine.getEntry(newAddress);
		assertEquals(newValue, entry.getValue());
		assertEquals(newTime, entry.getEvictTime());

		String newLongValue = "REPLACE_NEW_LONG_TEST_VALUE_REPLACE_NEW_LONG_TEST_VALUE_REPLACE_NEW_LONG_TEST_VALUE_REPLACE_NEW_LONG_TEST_VALUE";
		long newTime2 = new Date().getTime() + 1234;
		long newAddress2 = engine.replace(address, key, newValue, newLongValue, newTime2);

		entry = engine.getEntry(newAddress2);
		assertEquals(newLongValue, entry.getValue());
		assertEquals(newTime2, entry.getEvictTime());
		assertEquals(256, entry.getMaxSize());

		String wrongValue = "FAIL";
		long newTime3 = new Date().getTime() + 1234;
		long newAddress3 = engine.replace(address, key, wrongValue, wrongValue, newTime3);
		assertEquals(0L, newAddress3);

		assertEquals(true, engine.remove(key));
	}

	/*
	 * test method : evict()
	 */
	@Test
	public void evictTest() {
		String key = "EVICT_TEST_KEY";
		String value = "EVICT_TEST_VALUE";
		long time = new Date().getTime();

		long address = engine.add(key, value, time);
		assertEquals(address, engine.findAddress(key));
		engine.evict(new EvictItem(time, address));
		assertEquals(0, engine.findAddress(key));
	}

	/*
	 * test method : getKeys()
	 */
	@Test
	public void keySetTest() {
		String[] keys = { "0", "1", "2", "3", "4", "5", "6" };

		engine.add(keys[0], "");
		engine.add(keys[3], "a");
		engine.add(keys[2], "b");
		engine.add(keys[1], "c");
		engine.add(keys[5], "d");
		engine.add(keys[4], "e");
		engine.add(keys[6], "f");

		Iterator<String> keyItr = engine.getKeys();
		int i = 0;
		while (keyItr.hasNext()) {
			String key = keyItr.next();
			assertEquals(keys[i++], key);
		}

		assertEquals(true, engine.remove(keys[0]));
		assertEquals(true, engine.remove(keys[3]));
		assertEquals(true, engine.remove(keys[2]));
		assertEquals(true, engine.remove(keys[1]));
		assertEquals(true, engine.remove(keys[5]));
		assertEquals(true, engine.remove(keys[4]));
		assertEquals(true, engine.remove(keys[6]));
	}

	/*
	 * test method : clear()
	 */
	@Test
	public void clearTest() {
		String[] keys = { "0", "1", "2", "3", "4", "5", "6" };

		engine.add(keys[0], "");
		engine.add(keys[3], "a");
		engine.add(keys[2], "b");
		engine.add(keys[1], "c");
		engine.add(keys[5], "d");
		engine.add(keys[4], "e");
		engine.add(keys[6], "f");

		engine.clear();

		assertEquals(0, engine.findAddress(keys[0]));
		assertEquals(0, engine.findAddress(keys[3]));
		assertEquals(0, engine.findAddress(keys[2]));
		assertEquals(0, engine.findAddress(keys[1]));
		assertEquals(0, engine.findAddress(keys[5]));
		assertEquals(0, engine.findAddress(keys[4]));
		assertEquals(0, engine.findAddress(keys[6]));

		long address1 = engine.add(keys[0], "");
		long address2 = engine.add(keys[3], "a");
		long address3 = engine.add(keys[2], "b");
		long address4 = engine.add(keys[1], "c");
		long address5 = engine.add(keys[5], "d");
		long address6 = engine.add(keys[4], "e");
		long address7 = engine.add(keys[6], "f");

		assertEquals(address1, engine.findAddress(keys[0]));
		assertEquals(address2, engine.findAddress(keys[3]));
		assertEquals(address3, engine.findAddress(keys[2]));
		assertEquals(address4, engine.findAddress(keys[1]));
		assertEquals(address5, engine.findAddress(keys[5]));
		assertEquals(address6, engine.findAddress(keys[4]));
		assertEquals(address7, engine.findAddress(keys[6]));

		engine.clear();
	}

	/*
	 * test method : close()
	 */
	@Test
	public void closeTest() {
		EntryStorageManager<String, String> manager = new EntryStorageManagerImpl<String, String>(256,
				Serialize.STRING, Serialize.STRING);
		ReferenceStorage<String, String> engine2 = new ReferenceStorage<String, String>(128, manager);

		String[] keys = { "0", "1", "2", "3", "4", "5", "6" };

		engine2.add(keys[0], "");
		engine2.add(keys[3], "a");
		engine2.add(keys[2], "b");
		engine2.add(keys[1], "c");
		engine2.add(keys[5], "d");
		engine2.add(keys[4], "e");
		engine2.add(keys[6], "f");

		engine2.close();

		try {
			engine2.add(keys[0], "");
		} catch (Exception e) {
			assertEquals("a storage was closed", e.getMessage());
		}

		try {
			engine2.updateValue(1234, keys[0], "e", 0L);
		} catch (Exception e) {
			assertEquals("a storage was closed", e.getMessage());
		}

		try {
			engine2.replace(1234, keys[0], "e", "e", 0L);
		} catch (Exception e) {
			assertEquals("a storage was closed", e.getMessage());
		}

		try {
			engine2.findAddress(keys[0]);
		} catch (Exception e) {
			assertEquals("a storage was closed", e.getMessage());
		}

		try {
			engine2.remove(keys[0]);
		} catch (Exception e) {
			assertEquals("a storage was closed", e.getMessage());
		}

		try {
			engine2.evict(new EvictItem(0, 0));
		} catch (Exception e) {
			assertEquals("a storage was closed", e.getMessage());
		}

		try {
			engine2.clear();
		} catch (Exception e) {
			assertEquals("a storage was closed", e.getMessage());
		}

		try {
			engine2.close();
		} catch (Exception e) {
			assertEquals("a storage was closed", e.getMessage());
		}

	}
}
