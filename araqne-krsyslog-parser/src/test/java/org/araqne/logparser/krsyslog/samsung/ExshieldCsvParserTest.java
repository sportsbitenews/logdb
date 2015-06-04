package org.araqne.logparser.krsyslog.samsung;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import static org.junit.Assert.*;

public class ExshieldCsvParserTest {
	@Test
	public void testAdmittedLog() {
		String line = "\n[LOG_ADMITTED] Low,2015-06-04 15:01:41,26,10.10.13.32,60888,182.162.202.179,80,006,132512,0,246,2015-06-04 14:57:35,Internal\n";
		Map<String, Object> m = new ExshieldCsvParser().parse(log(line));

		assertEquals("admitted", m.get("type"));
		assertEquals("Low", m.get("priority"));
		assertEquals("2015-06-04 15:01:41", m.get("e_time"));
		assertEquals("26", m.get("rule_id"));
		assertEquals("10.10.13.32", m.get("src_ip"));
		assertEquals(60888, m.get("src_port"));
		assertEquals("182.162.202.179", m.get("dst_ip"));
		assertEquals(80, m.get("dst_port"));
		assertEquals("006", m.get("protocol"));
		assertEquals(132512L, m.get("recv_byte"));
		assertEquals(0L, m.get("send_byte"));
		assertEquals(246, m.get("duration"));
		assertEquals("2015-06-04 14:57:35", m.get("s_time"));
		assertEquals("Internal", m.get("direction"));
	}

	@Test
	public void testDeniedLog() {
		String line = "\n[LOG_DENIED] Normal,2015-06-04 15:01:42,1,10.10.17.90,45682,182.118.17.8,65001, 17,DENY, ,1,Internal\n";
		Map<String, Object> m = new ExshieldCsvParser().parse(log(line));

		assertEquals("denied", m.get("type"));
		assertEquals("Normal", m.get("priority"));
		assertEquals("2015-06-04 15:01:42", m.get("timestamp"));
		assertEquals("1", m.get("rule_id"));
		assertEquals("10.10.17.90", m.get("src_ip"));
		assertEquals(45682, m.get("src_port"));
		assertEquals("182.118.17.8", m.get("dst_ip"));
		assertEquals(65001, m.get("dst_port"));
		assertEquals(" 17", m.get("protocol"));
		assertEquals("DENY", m.get("action"));
		assertEquals(" ", m.get("sig_no"));
		assertEquals(1, m.get("deny_cnt"));
		assertEquals("Internal", m.get("direction"));
	}

	private Map<String, Object> log(String line) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);
		return m;
	}
}
