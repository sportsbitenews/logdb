//package org.araqne.logdb.cep.engine;
//
//import static org.junit.Assert.*;
//
//import org.araqne.logdb.cep.EventKey;
//import org.junit.Test;
//
//public class EventKeyTest {
//
//	@Test
//	public void parseTest() {
//		EventKey evtkey = new EventKey("topic", "demo");
//		assertEquals(evtkey, EventKey.parse(EventKey.marshal(evtkey)));
//
//		EventKey evtkey2 = new EventKey("topic2", "demo2");
//		evtkey2.setHost("host");
//		assertEquals(evtkey2, EventKey.parse(EventKey.marshal(evtkey2)));
//		assertEquals("host", EventKey.parse(EventKey.marshal(evtkey2)).getHost());
//	}
//}
