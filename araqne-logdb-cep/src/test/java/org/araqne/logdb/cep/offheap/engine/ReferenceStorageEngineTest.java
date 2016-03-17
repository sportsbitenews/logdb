package org.araqne.logdb.cep.offheap.engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

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
		engine = new ReferenceStorageEngine<String, String>(Serialize.STRING, Serialize.STRING); // *
	}

	@After
	public void onClose() throws IOException {
		engine.close();
	}

	@Test
	public void getTest() {
		engine.add(0, "0", "");
		engine.add(1, "1", "a");
		engine.add(2, "2", "b");
		engine.add(3, "3", "c");
		engine.add(4, "4", "d");
		engine.add(5, "5", "e");
		engine.add(16777215, "last", "Z");

		Entry<String, String> entry = engine.get(0);
		assertEquals("0", entry.getKey());
		assertEquals("", entry.getValue());

		entry = engine.get(1);
		assertEquals("1", entry.getKey());
		assertEquals("a", entry.getValue());

		entry = engine.get(2);
		assertEquals("2", entry.getKey());
		assertEquals("b", entry.getValue());

		entry = engine.get(3);
		assertEquals("3", entry.getKey());
		assertEquals("c", entry.getValue());

		entry = engine.get(4);
		assertEquals("4", entry.getKey());
		assertEquals("d", entry.getValue());

		entry = engine.get(5);
		assertEquals("5", entry.getKey());
		assertEquals("e", entry.getValue());

		entry = engine.get(16777215);
		assertEquals("last", entry.getKey());
		assertEquals("Z", entry.getValue());
	}

	@Test
	public void nextTest() {
		engine.add(1125125133, "test0", "테스트0");
		engine.add(1125125133, "test1", "테스트1");
		engine.add(1125125133, "test2", "테스트2");
		engine.add(1125125133, "test3", "테스트3");
		engine.add(1125125133, "test4", "테스트4");
		engine.add(1125125133, "test5", "테스트5");
		engine.add(1125125133, "test6", "테스트6");
		engine.add(1125125133, "test7", "테스트7");
		engine.add(1125125133, "test8", "테스트8");
		engine.add(1125125133, "test9", "테스트9");

		Entry<String, String> entry = engine.get(1125125133);
		assertEquals("test9", entry.getKey());
		assertEquals("테스트9", entry.getValue());

		entry = engine.next(entry);
		assertEquals("test8", entry.getKey());
		assertEquals("테스트8", entry.getValue());

		entry = engine.next(entry);
		assertEquals("test7", entry.getKey());
		assertEquals("테스트7", entry.getValue());

		entry = engine.next(entry);
		assertEquals("test6", entry.getKey());
		assertEquals("테스트6", entry.getValue());

		entry = engine.next(entry);
		assertEquals("test5", entry.getKey());
		assertEquals("테스트5", entry.getValue());

		entry = engine.next(entry);
		assertEquals("test4", entry.getKey());
		assertEquals("테스트4", entry.getValue());

		entry = engine.next(entry);
		assertEquals("test3", entry.getKey());
		assertEquals("테스트3", entry.getValue());

		entry = engine.next(entry);
		assertEquals("test2", entry.getKey());
		assertEquals("테스트2", entry.getValue());

		entry = engine.next(entry);
		assertEquals("test1", entry.getKey());
		assertEquals("테스트1", entry.getValue());

		entry = engine.next(entry);
		assertEquals("test0", entry.getKey());
		assertEquals("테스트0", entry.getValue());

		entry = engine.next(entry);
		assertNull(entry);
	}

	@Test
	public void removeTest() {
		// 1
		engine.add(100001, "abcd", "1234");
		Entry<String, String> entry = engine.get(100001);
		assertEquals("abcd", entry.getKey());
		assertEquals("1234", entry.getValue());

		engine.remove(entry, null);
		entry = engine.get(100001);
		assertNull(entry);

		// 2
		engine.add(100002, "2", "5678");
		engine.add(100002, "1", "1234");

		Entry<String, String> entry1 = engine.get(100002);
		assertEquals("1", entry1.getKey());
		assertEquals("1234", entry1.getValue());

		Entry<String, String> entry2 = engine.next(entry1);
		assertEquals("2", entry2.getKey());
		assertEquals("5678", entry2.getValue());

		engine.remove(entry2, entry1);
		entry1 = engine.get(100002);
		assertEquals("1", entry1.getKey());
		assertEquals("1234", entry1.getValue());

		entry2 = engine.next(entry1);
		assertNull(entry2);

		engine.add(100002, "2", "5678");
		entry1 = engine.get(100002);
		assertEquals("2", entry1.getKey());
		assertEquals("5678", entry1.getValue());

		entry2 = engine.next(entry1);
		assertEquals("1", entry2.getKey());
		assertEquals("1234", entry2.getValue());

		engine.remove(entry1, null);
		entry1 = engine.get(100002);
		assertEquals("1", entry1.getKey());
		assertEquals("1234", entry1.getValue());

		entry2 = engine.next(entry1);
		assertNull(entry2);

		engine.remove(entry1, null);
		entry1 = engine.get(100002);
		assertNull(entry1);

		// 3
		engine.add(100003, "3", "c");
		engine.add(100003, "2", "b");
		engine.add(100003, "1", "a");

		entry1 = engine.get(100003);
		assertEquals("1", entry1.getKey());
		assertEquals("a", entry1.getValue());

		entry2 = engine.next(entry1);
		assertEquals("2", entry2.getKey());
		assertEquals("b", entry2.getValue());

		Entry<String, String> entry3 = engine.next(entry2);
		assertEquals("3", entry3.getKey());
		assertEquals("c", entry3.getValue());

		engine.remove(entry2, entry1);
		entry1 = engine.get(100003);
		assertEquals("1", entry1.getKey());
		assertEquals("a", entry1.getValue());

		entry2 = engine.next(entry1);
		assertEquals("3", entry2.getKey());
		assertEquals("c", entry2.getValue());

		entry3 = engine.next(entry2);
		assertNull(entry3);
	}

	@Test
	public void updateTest() {
		engine.add(999, "abcd", "1234");
		Entry<String, String> entry = engine.get(999);
		assertEquals("abcd", entry.getKey());
		assertEquals("1234", entry.getValue());

		engine.update(entry, null, "5678");

		entry = engine.get(999);
		assertEquals("abcd", entry.getKey());
		assertEquals("5678", entry.getValue());
	}

	@Test
	public void clearTest() {
		engine.add(111, "!", "1");
		Entry<String, String> entry = engine.get(111);
		assertEquals("!", entry.getKey());
		assertEquals("1", entry.getValue());

		engine.clear();
		entry = engine.get(111);
		assertNull(entry);
	}

	@Test
	public void keySetTest() {
		String[] keys = { "0", "1", "2", "3", "4", "5", "6", "last" };

		engine.add(0, keys[0], "");
		engine.add(1, keys[3], "a");
		engine.add(1, keys[2], "b");
		engine.add(1, keys[1], "c");
		engine.add(4, keys[5], "d");
		engine.add(4, keys[4], "e");
		engine.add(5, keys[6], "f");
		engine.add(16777215, keys[7], "Z");

		Iterator<String> keyItr = engine.getKeys();
		int i = 0;
		while (keyItr.hasNext()) {
			assertEquals(keys[i++], keyItr.next());
		}
		engine.clear();
	}

	@Test
	public void loadTest() {
		int index = 125491;

		long address = engine.add(index, "key", "value");
		Entry<String, String> e1 = engine.get(index);
		Entry<String, String> e2 = engine.load(address);
		String key = engine.loadKey(address);
		String value = engine.loadValue(address);

		assertEquals("key", e1.getKey());
		assertEquals("key", e2.getKey());
		assertEquals("key", key);

		assertEquals("value", e1.getValue());
		assertEquals("value", e2.getValue());
		assertEquals("value", value);
	}
	
	@Test
	public void addExpireTest() {
		long timeout =  new Date().getTime();
	
		long address = engine.add(62415, "Key", "Value", timeout);
		Entry<String, String> entry = engine.get(62415);
		assertEquals(timeout, entry.getTimeoutTime());
		
		engine.evict(new TimeoutItem(timeout, address));
		Entry<String, String> entry2 = engine.get(62415);
		assertNull(entry2);
	}

}
