package org.araqne.logdb.cep.offheap.engine;

import static org.junit.Assert.assertEquals;

import org.araqne.logdb.cep.offheap.engine.serialize.Serialize;
import org.junit.Test;

public class EntryTest {

	@Test
	public void encodingTest() {
		Entry<String, String> original = new Entry<String, String>("key", "value", 1234, 32L, 125125L, 42L);
//		byte[] b = Entry.encodeEntry(original, Serialize.STRING, Serialize.STRING);
//		System.out.println(b.length);
//		Entry<String, String> decoded = Entry.decodeEntry(b, Serialize.STRING, Serialize.STRING, 42L);
//	
//		assertEquals(original.getKey(), decoded.getKey());
//		assertEquals(original.getValue(), decoded.getValue());
//		assertEquals(original.getHash(), decoded.getHash());
//		assertEquals(original.getNext(), decoded.getNext());
//		assertEquals(original.getAddress(), decoded.getAddress());
//		assertEquals(original.getTimeoutTime(), decoded.getTimeoutTime());
	}

}
