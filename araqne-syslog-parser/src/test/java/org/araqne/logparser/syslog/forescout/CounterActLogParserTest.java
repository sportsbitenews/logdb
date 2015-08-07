package org.araqne.logparser.syslog.forescout;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class CounterActLogParserTest {
	@Test
	public void testSample() {
		String line = "NAC-INTERNET[19573]: NAC Policy Log: Source: 211.193.201.235, Rule: Policy \"PC On/Off 확인(엔진기계_04:30)\" , Details: Host cleared from policy. Status was \"PC On/Off 확인(엔진기계_04:30):Unmatched\". Reason: Host identity changed.";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		CounterActLogParser p = new CounterActLogParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("INTERNET", m.get("nac_name"));
		assertEquals("NAC Policy Log", m.get("nac_log_type"));
		assertEquals("PC On/Off 확인(엔진기계_04:30)", m.get("nac_rule"));
	}

	@Test
	public void testSample2() {
		String line = "Jul 13 15:50:01 HHI-EM CROND[24534]: (root) CMD (/usr/lib/sa/sa1 1 1)";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		CounterActLogParser p = new CounterActLogParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("Jul 13 15:50:01", m.get("time"));
		assertEquals("HHI-EM", m.get("nac_name"));
		assertEquals("CROND[24534]: (root) CMD (/usr/lib/sa/sa1 1 1)", m.get("description"));
	}

	@Test
	public void testSample3() {
		String line = "NAC-JOSUN[10870]: Block Event: Host: 10.25.11.171, Target: 10.100.37.56, Time 1437628591, Service: 2186/TCP, Is Virtual Firewall blocking rule: true, Reason: Virtual Firewall - Limit Inbound";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		CounterActLogParser p = new CounterActLogParser();
		Map<String, Object> m = p.parse(log);
	}
}
