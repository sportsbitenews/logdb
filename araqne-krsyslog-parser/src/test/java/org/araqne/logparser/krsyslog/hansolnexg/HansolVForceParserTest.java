package org.araqne.logparser.krsyslog.hansolnexg;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class HansolVForceParserTest {
	@Test
	public void testSample() {
		String line = "Apr 14 03:19:34 session: NZC25081310046 Proto:1, Policy:pass, Rule:1, Type:open, Src:10.10.81.16, Dst:10.10.81.1, Spt_c:0, Dpt_t:8, Bytes:0, Packets:0, Repl_Src:10.10.81.1, Repl_Dst:10.10.81.16, Repl_Spt_c:0, Repl_Dpt_t:0, Repl_Bytes:0, Repl_Packets:0, Count:1, Start_Time:Apr 14 03:19:34, End_Time:-";
		
		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		HansolVForceParser p = new HansolVForceParser();
		Map<String, Object> m = p.parse(log);
		
		assertEquals("Apr 14 03:19:34", m.get("DateTime"));
		assertEquals("session", m.get("Log_Category"));
		assertEquals("1", m.get("Proto"));
		assertEquals("10.10.81.16", m.get("Src"));
		assertEquals("Apr 14 03:19:34", m.get("Start_Time"));
		assertEquals("-", m.get("End_Time"));
	}
	
	@Test
	public void testSample2() {
		String line = "Apr 14 17:42:53 appgw_http: NZC25081310046 Src:10.10.81.49, Dst:23.76.153.59, Action:pass, Name:http-filter-test-1, Rule:9999, Host:download.cdn.mozilla.net, Path:/pub/firefox/releases/37.0.1/update/win32/ko/firefox-37.0.1.complete.mar";
		
		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		HansolVForceParser p = new HansolVForceParser();
		Map<String, Object> m = p.parse(log);
		
		assertEquals("Apr 14 17:42:53", m.get("DateTime"));
		assertEquals("appgw_http", m.get("Log_Category"));
		assertEquals("http-filter-test-1", m.get("Name"));
		assertEquals("/pub/firefox/releases/37.0.1/update/win32/ko/firefox-37.0.1.complete.mar", m.get("Path"));
	}
}
