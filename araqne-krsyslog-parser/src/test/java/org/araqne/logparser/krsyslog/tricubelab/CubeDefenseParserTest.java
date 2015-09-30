package org.araqne.logparser.krsyslog.tricubelab;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class CubeDefenseParserTest {
	@Test
	public void testSample() {
		String line = "Dec  4 11:21:09 cube1g CubeDefense[19501]: 0,\"http://cdn.nt.com/c.js\",\"http://www.g.or.kr/home/\",GET,73.223.217.35,80,192.168.0.91,34060,355,3623,K,1,0";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		CubeDefenseParser p = new CubeDefenseParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("Dec  4 11:21:09", m.get("datetime"));
		assertEquals("http://cdn.nt.com/c.js", m.get("url"));
		assertEquals("GET", m.get("http_request_method"));
	}

	@Test
	public void testSample2() {
		String line = "Dec  4 11:16:38 cube1g CubeDefense[19501]: 0,\"www.mins.com\",\"\",,62.116.143.210,21,192.168.11.197,48003,74,4346,K,3,1";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		CubeDefenseParser p = new CubeDefenseParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("Dec  4 11:16:38", m.get("datetime"));
		assertEquals("CubeDefense[19501]", m.get("event_id"));
		assertEquals("www.mins.com", m.get("url"));
		assertEquals("", m.get("refer"));
	}
}
