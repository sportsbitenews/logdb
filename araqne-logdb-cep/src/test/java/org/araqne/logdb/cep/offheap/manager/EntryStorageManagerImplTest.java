package org.araqne.logdb.cep.offheap.manager;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Date;

import org.araqne.logdb.cep.offheap.engine.Entry;
import org.araqne.logdb.cep.offheap.engine.serialize.Serialize;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class EntryStorageManagerImplTest {

	private EntryStorageManagerImpl<String, String> manager;

	@Before
	public void onStart() {
		manager = new EntryStorageManagerImpl<String, String>(64, Serialize.STRING, Serialize.STRING); // *
	}

	@After
	public void onClose() throws IOException {
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
		long address = manager.set(entry);

		byte[] keyByte = Serialize.STRING.serialize(key);
		byte[] valueByte = Serialize.STRING.serialize(value);

		assertEquals(hash, manager.getHash(address));
		assertEquals(next, manager.getNext(address));
		assertEquals(timeoutTime, manager.getEvictTime(address));
		assertEquals(keyByte.length, manager.getKeySize(address));
		assertEquals(valueByte.length, manager.getValueSize(address));
		assertEquals(64, manager.getMaxSize(address));
		assertEquals(key, manager.getKey(address));
		assertEquals(value, manager.getValue(address));

		Entry<String, String> entry2 = manager.get(address);
		assertEquals(hash, entry2.getHash());
		assertEquals(next, entry2.getNext());
		assertEquals(timeoutTime, entry2.getEvictTime());
		assertEquals(64, entry2.getMaxSize());
		assertEquals(key, entry2.getKey());
		assertEquals(value, entry2.getValue());

		manager.remove(address);
	}

	/*
	 * test method : updateValue
	 */
	@Test
	public void updateValueTest() {
		String key = "UPDATE_KEY";
		String value = "UPDATE_VALUE";
		int hash = key.hashCode();
		long next = 125156L;
		long timeoutTime = new Date().getTime();
		Entry<String, String> entry = new Entry<String, String>(key, value, hash, next, timeoutTime);
		long address = manager.set(entry);

		String newValue = "NEW_UPDAATE_VALUE";
		long newAddress = manager.updateValue(address, newValue, timeoutTime);
		Entry<String, String> entry2 = manager.get(newAddress);
		assertEquals(newValue, entry2.getValue());

		String newLongValue = "NEW_LONG_UPDATE_VALUE_NEW_LONG_UPDATE_VALUE_NEW_LONG_UPDATE_VALUE_NEW_LONG_UPDATE_VALUE";
		long newAddress2 = manager.updateValue(newAddress, newLongValue, timeoutTime);
		Entry<String, String> entry3 = manager.get(newAddress2);
		assertEquals(newLongValue, entry3.getValue());
		manager.remove(newAddress2);
	}

	/*
	 * test method : replaceValue()
	 */
	@Test
	public void replaceValue() {
		String key = "REPLACE_KEY";
		String value = "REPLACE_VALUE";
		int hash = value.hashCode();
		long next = 125156L;
		long evictTime = 0L;
		Entry<String, String> entry = new Entry<String, String>(key, value, hash, next, evictTime);
		long address = manager.set(entry);

		// success
		String newValue = "NEW_REPLACE_VALUE";
		long newEvictTime = 1000L;
		long newAddress = manager.replaceValue(address, value, newValue, newEvictTime);
		Entry<String, String> entry2 = manager.get(newAddress);

		assertEquals(entry.getHash(), entry2.getHash());
		assertEquals(entry.getNext(), entry2.getNext());
		assertEquals(entry.getMaxSize(), entry2.getMaxSize());
		assertEquals(newEvictTime, entry2.getEvictTime());
		assertEquals(entry.getKey(), entry2.getKey());
		assertEquals(newValue, entry2.getValue());

		// success & reallocate
		String newLongValue = "NEW_REPLACE_VALUE_NEW_REPLACE_VALUE_NEW_REPLACE_VALUE_NEW_REPLACE_VALUE_NEW_REPLACE_VALUE_NEW_REPLACE_VALUE_NEW_REPLACE_VALUE_";
		long newAddress2 = manager.replaceValue(address, newValue, newLongValue, 0L);
		Entry<String, String> entry3 = manager.get(newAddress2);
		assertEquals(newLongValue, entry3.getValue());
		assertEquals(newEvictTime, entry3.getEvictTime());

		// fail
		long newAddress3 = manager.replaceValue(newAddress2, newValue, newLongValue, 0L);
		System.out.println(newAddress3);
		assertEquals(0L, newAddress3);
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
		long address = manager.set(entry);

		assertEquals(false, manager.equalsHash(address, 0));
		assertEquals(true, manager.equalsHash(address, hash));
		assertEquals(false, manager.equalsKey(address, "WRONG_KEY"));
		assertEquals(true, manager.equalsKey(address, key));

		manager.remove(address);
	}

}
