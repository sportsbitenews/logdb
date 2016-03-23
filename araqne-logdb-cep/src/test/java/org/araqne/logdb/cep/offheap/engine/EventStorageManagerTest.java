package org.araqne.logdb.cep.offheap.engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;

import org.araqne.logdb.cep.offheap.engine.serialize.Serialize;
import org.araqne.logdb.cep.offheap.storage.EntryStorageManager;
import org.araqne.logdb.cep.offheap.timeout.TimeoutItem;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class EventStorageManagerTest {

	private EntryStorageManager<String, String> manager;

	@Before
	public void onStart() {
		manager = new EntryStorageManager<String, String>(64, Serialize.STRING, Serialize.STRING); // *
	}

	@After
	public void onClose() throws IOException {
		manager.clear();
	}

	/*
	 * test method : putEntry(), get(), getHash(), getNext(), getXXX()
	 */
	@Test
	public void basicTest() {
		String key = "TEST_KEY";
		String value = "TEST_VALUE";
		int hash = key.hashCode();
		long next = 125156L;
		long timeoutTime = new Date().getTime();
		Entry<String, String> entry = new Entry<String, String>(key, value, hash, next, timeoutTime);
		long address = manager.putEntry(entry);

		byte[] keyByte = Serialize.STRING.serialize(key);
		byte[] valueByte = Serialize.STRING.serialize(value);

		assertEquals(hash, manager.getHash(address));
		assertEquals(next, manager.getNext(address));
		assertEquals(timeoutTime, manager.getTimeoutTime(address));
		assertEquals(keyByte.length, manager.getKeySize(address));
		assertEquals(valueByte.length, manager.getValueSize(address));
		assertEquals(64, manager.getMaxSize(address));
		assertEquals(key, manager.getKey(address));
		assertEquals(value, manager.getValue(address));

		Entry<String, String> entry2 = manager.get(address);
		assertEquals(hash, entry2.getHash());
		assertEquals(next, entry2.getNext());
		assertEquals(timeoutTime, entry2.getTimeoutTime());
		assertEquals(64, entry2.getMaxSize());
		assertEquals(key, entry2.getKey());
		assertEquals(value, entry2.getValue());

		manager.remove(address);
	}

	/*
	 * test method : replaceValue
	 */
	@Test
	public void replaceValueTest() {
		String key = "REPLACE_KEY";
		String value = "REPLACE_VALUE";
		int hash = value.hashCode();
		long next = 125156L;
		long timeoutTime = new Date().getTime();
		Entry<String, String> entry = new Entry<String, String>(key, value, hash, next, timeoutTime);
		long address = manager.putEntry(entry);

		String newValue = "NEW_REPLACE_VALUE";
		long newAddress = manager.replaceValue(address, newValue, timeoutTime);
		Entry<String, String> entry2 = manager.get(newAddress);
		assertEquals(newValue, entry2.getValue());

		String newLongValue = "NEW_Long_REPLACE_VALUE_NEW_Long_REPLACE_VALUE_NEW_Long_REPLACE_VALUE";
		long newAddress2 = manager.replaceValue(newAddress, newLongValue, timeoutTime);
		Entry<String, String> entry3 = manager.get(newAddress2);
		assertEquals(newLongValue, entry3.getValue());
		manager.remove(newAddress2);
	}

	/*
	 * test method : equalsHash(), equalsKey()
	 */
	@Test
	public void equalsTest() {
		String key = "EQUALS_KEY";
		String value = "EQUALS_VALUE";
		int hash = value.hashCode();
		long next = 125156L;
		long timeoutTime = new Date().getTime();
		Entry<String, String> entry = new Entry<String, String>(key, value, hash, next, timeoutTime);
		long address = manager.putEntry(entry);

		assertEquals(false, manager.equalsHash(address, 0));
		assertEquals(true, manager.equalsHash(address, hash));
		assertEquals(false, manager.equalsKey(address, "WRONG_KEY"));
		assertEquals(true, manager.equalsKey(address, key));

		manager.remove(address);
	}

}
