package org.araqne.logparser.krsyslog.kornicglory;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class TessAuditParserTest {
	@Test
	public void test() {
		String line = "LAUDITLOGINDEX=42056\nLTYPE2=7\nLAUDITSETINDEX=178\nSTRCONTENT=분석";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		TessAuditParser p = new TessAuditParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("42056", m.get("audit_log_index"));
		assertEquals("7", m.get("type2"));
		assertEquals("178", m.get("audit_set_index"));
		assertEquals("분석", m.get("str_content"));
	}

	@Test
	public void test2() {
		String line = "TMOCCUR=2013-03-27 16:05:21\nLTYPE1=3\nSTROPERATOR=호호\n************";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		TessAuditParser p = new TessAuditParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("2013-03-27 16:05:21", m.get("occur_time"));
		assertEquals("3", m.get("type1"));
		assertEquals("호호", m.get("str_operator"));
	}
}
