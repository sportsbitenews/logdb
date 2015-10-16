package org.araqne.logparser.krsyslog.tricubelab;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class CubeDefenseParserTest {
	@Test
	public void testSample() {
		String line = "Dec  4 11:21:09 cube1g CubeDefense[19501]: 0,\"http://werwer.com/c.js\",\"http://www.wer.or.kr/home/\",GET,73.223.17.35,80,192.168.0.11,34060,355,3623,K,1,0";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		CubeDefenseParser p = new CubeDefenseParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("Dec  4 11:21:09", m.get("datetime"));
		assertEquals("CubeDefense[19501]", m.get("event_id"));
		assertEquals("0", m.get("dev_no"));
		assertEquals("http://werwer.com/c.js", m.get("url"));
		assertEquals("http://www.wer.or.kr/home/", m.get("refer"));
		assertEquals("GET", m.get("http_request_method"));
		assertEquals("73.223.17.35", m.get("dst_ip"));
		assertEquals("80", m.get("dst_port"));
		assertEquals("192.168.0.11", m.get("src_ip"));
		assertEquals("34060", m.get("src_port"));
		assertEquals("355", m.get("packet_length"));
		assertEquals("3623", m.get("pattern_id"));
		assertEquals("K", m.get("pattern_source"));
		assertEquals("1", m.get("pattern_type"));
		assertEquals("0", m.get("is_pattern_detect_ignored"));
	}

	@Test
	public void testSample2() {
		String line = "Dec  4 11:16:38 cube1g CubeDefense[19501]: 0,\"www.werms.com\",\"\",,62.116.143.10,21,192.168.11.97,48003,74,4346,K,3,1";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		CubeDefenseParser p = new CubeDefenseParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("Dec  4 11:16:38", m.get("datetime"));
		assertEquals("CubeDefense[19501]", m.get("event_id"));
		assertEquals("0", m.get("dev_no"));
		assertEquals("www.werms.com", m.get("url"));
		assertEquals("", m.get("refer"));
		assertEquals("", m.get("http_request_method"));
		assertEquals("62.116.143.10", m.get("dst_ip"));
		assertEquals("21", m.get("dst_port"));
		assertEquals("192.168.11.97", m.get("src_ip"));
		assertEquals("48003", m.get("src_port"));
		assertEquals("74", m.get("packet_length"));
		assertEquals("4346", m.get("pattern_id"));
		assertEquals("K", m.get("pattern_source"));
		assertEquals("3", m.get("pattern_type"));
		assertEquals("1", m.get("is_pattern_detect_ignored"));
	}
}
