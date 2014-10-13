package org.araqne.logparser.krsyslog.futuresystems;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.araqne.logparser.krsyslog.futuresystems.WeguardiaLogParser;
import org.junit.Test;

import static org.junit.Assert.*;

public class WeguardiaLogParserTest {
	@Test
	public void parse() {
		String line = "0;20121205 114609; ;172.16.0.97;110.92.155.254;5318;5318;121.189.14.140;121.189.14.140;80;80;6;1;537133067;4;arko-guro;Close[00:00:20. SF. FIN] NAT[313] R[16]; ; ;1; ;1698;;eth3;";
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);

		Map<String, Object> p = new WeguardiaLogParser().parse(m);

		assertEquals("0", p.get("type"));
		SimpleDateFormat f = new SimpleDateFormat("yyyyMMdd HHmmss");
		assertEquals("20121205 114609", f.format(p.get("date")));
		assertEquals(" ", p.get("oip"));
		assertEquals("172.16.0.97", p.get("sip"));
		assertEquals("110.92.155.254", p.get("nat_sip"));
		assertEquals(5318, p.get("sport"));
		assertEquals(5318, p.get("nat_sport"));
		assertEquals("121.189.14.140", p.get("dip"));
		assertEquals("121.189.14.140", p.get("nat_dip"));
		assertEquals(80, p.get("dport"));
		assertEquals(80, p.get("nat_dport"));
		assertEquals("6", p.get("protocol"));
		assertEquals("1", p.get("logtype"));
		assertEquals("537133067", p.get("act"));
		assertEquals(4, p.get("severity"));
		assertEquals("arko-guro", p.get("product"));
		assertEquals("Close[00:00:20. SF. FIN] NAT[313] R[16]", p.get("note"));
		assertEquals(1, p.get("count"));
		assertEquals(" ", p.get("category"));
		assertEquals("1", p.get("rule"));
		assertEquals(" ", p.get("group_id"));
		assertEquals(1698L, p.get("usage"));
		assertEquals("eth3", p.get("iface"));
	}
}
