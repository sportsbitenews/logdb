package org.araqne.logparser.krsyslog.monitorlab;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class WisgParserTest {
	@Test
	public void testSystemLog() {
		String line = "SYS|20100818160800|10.0.1.122|1|16|8|10|14735|40333|62|72";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		WisgParser p = new WisgParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("SYS", m.get("log_type"));
		assertEquals("20100818160800", m.get("time"));
		assertEquals("10.0.1.122", m.get("gateway"));
		assertEquals("1", m.get("cpu"));
		assertEquals("16", m.get("memory"));
		assertEquals("8", m.get("cps"));
		assertEquals("10", m.get("tps"));
		assertEquals("14735", m.get("inbound_kbyte_persec"));
		assertEquals("40333", m.get("outbound_kbyte_persec"));
		assertEquals("62", m.get("inbound_pps"));
		assertEquals("72", m.get("outbound_pps"));
	}

	@Test
	public void testCommonLog() {
		String line = "20130220174924|1.215.87.29|20936|211.193.193.65|80|10.1.1.100|Unpermitted HTTP Method|0|Unpermitted HTTP Method: OPTIONS|DETECT|LOW|HTTP|ehitis.hhi.co.kr|154|OPTIONS /v2.0/Doc_trans/201302 HTTP/1.1\nConnection: Keep-Alive\nUser-Agent: Microsoft-WebDAV-MiniRedir/6.1.7601\ntranslate: f\nHost: ehitis.hhi.co.kr";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		WisgParser p = new WisgParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("20130220174924", m.get("time"));
		assertEquals("1.215.87.29", m.get("client_ip"));
		assertEquals("20936", m.get("client_port"));
		assertEquals("211.193.193.65", m.get("server_ip"));
		assertEquals("80", m.get("server_port"));
		assertEquals("10.1.1.100", m.get("gateway"));
		assertEquals("Unpermitted HTTP Method", m.get("detect_classification"));
		assertEquals("0", m.get("rule_id"));
		assertEquals("Unpermitted HTTP Method: OPTIONS", m.get("detect_base"));
		assertEquals("DETECT", m.get("detect_result"));
		assertEquals("LOW", m.get("risk_level"));
		assertEquals("HTTP", m.get("protocol"));
		assertEquals("ehitis.hhi.co.kr", m.get("host"));
		assertEquals("154", m.get("request_length"));
		assertEquals("OPTIONS /v2.0/Doc_trans/201302 HTTP/1.1\nConnection: Keep-Alive\nUser-Agent: Microsoft-WebDAV-MiniRedir/6.1.7601\ntranslate: f\nHost: ehitis.hhi.co.kr", m.get("request_data"));
	}
}
