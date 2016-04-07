package org.araqne.logdb.cep.offheap.manager;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Date;

import org.araqne.logdb.Row;
import org.araqne.logdb.cep.EventContext;
import org.araqne.logdb.cep.EventKey;
import org.araqne.logdb.cep.offheap.engine.Entry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class EventEntryStorageManagerTest {

	private EntryStorageManager<EventKey, EventContext> manager;
	private EventKey key;
	private EventContext value;

	@Before
	public void onStart() {
		manager = new EventEntryStorageManager(256);
		key = new EventKey("topic", "key", "host");
		value = new EventContext(key, new Date().getTime(), new Date().getTime() + 1000, new Date().getTime() + 2000,
				10);
		value.getCounter().set(1);
		Row row = new Row();
		row.put("_table", "t");
		row.put("_id", 999999);
		row.put("_time", "2016-03-10 19:47:07+0900");
		row.put("key", "999999");
		value.addRow(row);
		value.setVariable("ip", "127.0.0.1");
		value.setVariable("name", "kim");
	}

	@After
	public void onClose() throws IOException {
	}

	/*
	 * test method : set(), get(), remove()
	 */
	@Test
	public void getAndSetTest() {
		int hash = key.hashCode();
		long next = 125156L;
		long evictTime = value.getExpireTime();
		Entry<EventKey, EventContext> entry = new Entry<EventKey, EventContext>(key, value, hash, next, evictTime);
		long address = manager.set(entry);

		Entry<EventKey, EventContext> entry2 = manager.get(address);
		assertEquals(entry.getHash(), entry2.getHash());
		assertEquals(entry.getNext(), entry2.getNext());
		assertEquals(entry.getEvictTime(), entry2.getEvictTime());
		assertEquals(entry.getMaxSize(), entry2.getMaxSize());
		assertEquals(entry.getKey(), entry2.getKey());
		assertEquals(entry.getValue(), entry2.getValue());

		manager.remove(address);
	}

	/*
	 * test method : updateValue
	 */
	@Test
	public void updateValueTest() {
		int hash = value.hashCode();
		long next = 125156L;
		long evictTime = value.getExpireTime();
		Entry<EventKey, EventContext> entry = new Entry<EventKey, EventContext>(key, value, hash, next, evictTime);
		long address = manager.set(entry);

		EventContext newValue = new EventContext(key, new Date().getTime(), new Date().getTime() + 2000,
				new Date().getTime() + 1000, 10);
		value.getCounter().set(1);
		Row row1 = new Row();
		row1.put("_table", "t");
		row1.put("_id", 999999);
		row1.put("_time", "2016-03-10 19:47:07+0900");
		row1.put("key", "999999");
		row1.put("new key", "new value");
		newValue.addRow(row1);
		newValue.setVariable("new ip", "127.0.0.2");

		// when size < maxSize
		long newAddress = manager.updateValue(address, newValue, 0L);
		Entry<EventKey, EventContext> entry2 = manager.get(newAddress);

		assertEquals(entry.getHash(), entry2.getHash());
		assertEquals(entry.getNext(), entry2.getNext());
		assertEquals(entry.getMaxSize(), entry2.getMaxSize());
		assertEquals(newValue.getTimeoutTime(), entry2.getEvictTime());
		assertEquals(entry.getKey(), entry2.getKey());
		assertEquals(newValue, entry2.getValue());

		// when size > maxSize
		EventContext newLongValue = new EventContext(key, new Date().getTime(), new Date().getTime() + 2000,
				new Date().getTime() + 1000, 10);
		value.getCounter().set(1);
		Row row2 = new Row();
		row2.put("_table", "t");
		row2.put("_id", 999999);
		row2.put("_time", "2016-03-10 19:47:07+0900");
		row2.put("key", "999999");
		row2.put("LONG_TEST_KEY1", "LONG_TEST_KEY2");
		row2.put("LONG_TEST_KEY2", "LONG_TEST_KEY2");
		row2.put("LONG_TEST_KEY3", "LONG_TEST_KEY2");
		row2.put("LONG_TEST_KEY4", "LONG_TEST_KEY2");
		row2.put("LONG_TEST_KEY5", "LONG_TEST_KEY2");
		row2.put("LONG_TEST_KEY6", "LONG_TEST_KEY2");
		row2.put("LONG_TEST_KEY7", "LONG_TEST_KEY2");
		row2.put("LONG_TEST_KEY8", "LONG_TEST_KEY2");
		row2.put("LONG_TEST_KEY9", "LONG_TEST_KEY2");
		row2.put("LONG_TEST_KEY10", "LONG_TEST_KEY2");
		row2.put("LONG_TEST_KEY11", "LONG_TEST_KEY2");
		row2.put("LONG_TEST_KEY12", "LONG_TEST_KEY2");
		row2.put("LONG_TEST_KEY13", "LONG_TEST_KEY2");
		row2.put("LONG_TEST_KEY14", "LONG_TEST_KEY2");
		row2.put("LONG_TEST_KEY15", "LONG_TEST_KEY2");
		newLongValue.addRow(row2);
		newLongValue.setVariable("new ip", "127.0.0.3");

		long newAddress2 = manager.updateValue(newAddress, newLongValue, 0L);
		Entry<EventKey, EventContext> entry3 = manager.get(newAddress2);
		assertEquals(newLongValue, entry3.getValue());

		manager.remove(newAddress2);
	}

	/*
	 * test method : replaceValue()
	 */
	@Test
	public void replaceValue() {
		int hash = value.hashCode();
		long next = 125156L;
		long evictTime = value.getExpireTime();
		Entry<EventKey, EventContext> entry = new Entry<EventKey, EventContext>(key, value, hash, next, evictTime);
		long address = manager.set(entry);

		// success
		long created = new Date().getTime() + 5000;
		EventContext newValue = new EventContext(key, created, created + 2000, created + 1000, 10);
		value.getCounter().set(1);
		Row row1 = new Row();
		row1.put("_table", "t");
		row1.put("_id", 999999);
		row1.put("_time", "2016-03-10 19:47:07+0900");
		row1.put("key", "999999");
		row1.put("A", "a");
		newValue.addRow(row1);
		newValue.setVariable("new ip", "127.0.0.2");

		long newAddress = manager.replaceValue(address, value, newValue, 0L);
		Entry<EventKey, EventContext> entry2 = manager.get(newAddress);

		assertEquals(entry.getHash(), entry2.getHash());
		assertEquals(entry.getNext(), entry2.getNext());
		assertEquals(entry.getMaxSize(), entry2.getMaxSize());
		assertEquals(newValue.getTimeoutTime(), entry2.getEvictTime());
		assertEquals(entry.getKey(), entry2.getKey());
		assertEquals(newValue, entry2.getValue());

		// success & reallocate
		long created2 = new Date().getTime() + 100000;
		EventContext newLongValue = new EventContext(key, created2, created2 + 3000, created2 + 2000, 10);
		value.getCounter().set(1);
		Row row2 = new Row();
		row2.put("_table", "t");
		row2.put("_id", 999999);
		row2.put("_time", "2016-03-10 19:47:07+0900");
		row2.put("key", "999999");
		row2.put("LONG_TEST_KEY1", "LONG_TEST_KEY2");
		row2.put("LONG_TEST_KEY2", "LONG_TEST_KEY2");
		row2.put("LONG_TEST_KEY3", "LONG_TEST_KEY2");
		row2.put("LONG_TEST_KEY4", "LONG_TEST_KEY2");
		row2.put("LONG_TEST_KEY5", "LONG_TEST_KEY2");
		row2.put("LONG_TEST_KEY6", "LONG_TEST_KEY2");
		row2.put("LONG_TEST_KEY7", "LONG_TEST_KEY2");
		row2.put("LONG_TEST_KEY8", "LONG_TEST_KEY2");
		row2.put("LONG_TEST_KEY9", "LONG_TEST_KEY2");
		row2.put("LONG_TEST_KEY10", "LONG_TEST_KEY2");
		row2.put("LONG_TEST_KEY11", "LONG_TEST_KEY2");
		row2.put("LONG_TEST_KEY12", "LONG_TEST_KEY2");
		row2.put("LONG_TEST_KEY13", "LONG_TEST_KEY2");
		row2.put("LONG_TEST_KEY14", "LONG_TEST_KEY2");
		row2.put("LONG_TEST_KEY15", "LONG_TEST_KEY2");
		newLongValue.addRow(row2);
		newLongValue.setVariable("new ip", "127.0.0.3");

		long newAddress2 = manager.replaceValue(address, newValue, newLongValue, 0L);
		Entry<EventKey, EventContext> entry3 = manager.get(newAddress2);
		assertEquals(newLongValue, entry3.getValue());
		assertEquals(newLongValue.getTimeoutTime(), entry3.getEvictTime());

		// fail
		long newAddress3 = manager.replaceValue(newAddress2, newValue, newLongValue, 0L);
		assertEquals(0L, newAddress3);
		manager.remove(newAddress2);
	}

	/*
	 * test method : equalsHash(), equalsKey()
	 */
	@Test
	public void equalsTest() {
		int hash = value.hashCode();
		long next = 125156L;
		long evictTime = value.getExpireTime();
		Entry<EventKey, EventContext> entry = new Entry<EventKey, EventContext>(key, value, hash, next, evictTime);
		long address = manager.set(entry);

		assertEquals(false, manager.equalsHash(address, 0));
		assertEquals(true, manager.equalsHash(address, hash));
		assertEquals(false, manager.equalsKey(address, new EventKey("k", "e", "y")));
		assertEquals(true, manager.equalsKey(address, key));

		manager.remove(address);
	}

}
