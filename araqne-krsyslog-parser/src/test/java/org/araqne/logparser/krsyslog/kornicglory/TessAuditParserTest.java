package org.araqne.logparser.krsyslog.kornicglory;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class TessAuditParserTest {
	@Test
	public void test() {
		String line = "LAUDITLOGINDEX=42056\nLTYPE2=7\nLAUDITSETINDEX=178\nSTRCONTENT=패킷 분석에 대한 처리누수가 발생 하였습니다.(traffic gathering)";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		TessAuditParser p = new TessAuditParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("42056", m.get("lauditlogindex"));
		assertEquals("7", m.get("ltype2"));
		assertEquals("178", m.get("lauditsetindex"));
		assertEquals("패킷 분석에 대한 처리누수가 발생 하였습니다.(traffic gathering)", m.get("strcontent"));
	}

	@Test
	public void test2() {
		String line = "TMOCCUR=2013-03-27 16:05:21\nLTYPE1=3\nSTROPERATOR=센서(192.168.70.81)\n************";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		TessAuditParser p = new TessAuditParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("2013-03-27 16:05:21", m.get("tmoccur"));
		assertEquals("3", m.get("ltype1"));
		assertEquals("센서(192.168.70.81)", m.get("stroperator"));
	}
}
