package org.araqne.logparser.krsyslog.watchguard;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class WatchGuardParserTest {
	@Test
	public void testSample() {
		String line = "<140>Feb  6 18:46:32 Hizeaero_JinJu firewall: msg_id=\"3000-0148\" Deny 1-Trusted Firebox 32 udp 20 200 55.55.20.238 55.55.20.254 9000 9000  (Unhandled Internal Packet-00)";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		WatchGuardParser p = new WatchGuardParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("Feb  6 18:46:32", m.get("date_time"));
		assertEquals("Hizeaero_JinJu firewall", m.get("source"));
		assertEquals("msg_id=\"3000-0148\" Deny 1-Trusted Firebox 32 udp 20 200 55.55.20.238 55.55.20.254 9000 9000  (Unhandled Internal Packet-00)", m.get("message"));
	}
}
