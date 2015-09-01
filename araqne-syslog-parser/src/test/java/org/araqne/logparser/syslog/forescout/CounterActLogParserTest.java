package org.araqne.logparser.syslog.forescout;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class CounterActLogParserTest {
	@Test
	public void testSample() {
		String line = "NAC-HELLO[19573]: NAC Policy Log: Source: 1.2.3.4, Rule: Policy \"룰\" , Details: Host cleared from policy. Status was \"상태\". Reason: Host identity changed.";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		CounterActLogParser p = new CounterActLogParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("HELLO", m.get("nac_name"));
		assertEquals("NAC Policy Log", m.get("nac_log_type"));
		assertEquals("Policy \"룰\"", m.get("nac_rule"));
	}

	@Test
	public void testSample2() {
		String line = "Jul 13 15:50:01 HOHO desc";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		CounterActLogParser p = new CounterActLogParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("Jul 13 15:50:01", m.get("time"));
		assertEquals("HOHO", m.get("nac_name"));
		assertEquals("desc", m.get("description"));
	}

	@Test
	public void testSample3() {
		String line = "NAC-BYE[10870]: Block Event: Host: 1.2.3.4, Target: 5.6.7.8, Time 1437628591, Service: 1111/TCP, Is Virtual Firewall blocking rule: true, Reason: Virtual Firewall - Limit Inbound";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		CounterActLogParser p = new CounterActLogParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("BYE", m.get("nac_name"));
	}
}
