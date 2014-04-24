package org.araqne.logparser.syslog.fireeye;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.*;

public class FireeyeLogParserTest {
	@Test
	public void testMpsLog() {
		String line = "fenotify-42053.alert: LEEF:1.0|FireEye|MPS|7.0.0.138133|malware-callback|"
				+ "src=110.222.210.197^sname=Trojan.Msidebar.C^dstMAC=10:04:96:6d:71:5c^proto=tcp^"
				+ "dvchost=Bora-FireEye^dst=151.72.69.29^vlan=4093^srcPort=60545^"
				+ "dvc=151.200.99.22^cncHost=151.72.69.29^externalId=42053^"
				+ "devTime=Mar 18 2014 10:03:29 Z^sid=89046625^cncPort=80^"
				+ "link=https://Bora-FireEye./event_stream/events_for_bot?ev_id\\=42053^dstPort=80^"
				+ "cncChannel=GET /AppTag/TagCnt_xe2.asp HTTP/1.1::~~Host: ad.syndiapi.com::~~Accept: text/html, "
				+ "*/*::~~Accept-Encoding: identity::~~User-Agent: Mozilla/3.0 (compatible; Indy Library)::~~::~~^"
				+ "srcMAC=10:04:96:52:de:96^";
		
	
		Map<String, Object> input = new HashMap<String, Object>();
		input.put("line", line);

		Map<String, Object> m = new FireeyeLogParser().parse(input);
		assertEquals("7.0.0.138133", m.get("ver"));
		assertEquals("110.222.210.197", m.get("src"));
		assertEquals("Trojan.Msidebar.C", m.get("sname"));
		assertEquals("10:04:96:6d:71:5c", m.get("dstMAC"));
		assertEquals("tcp", m.get("proto"));
		assertEquals("Bora-FireEye", m.get("dvchost"));
		assertEquals("151.72.69.29", m.get("dst"));
		assertEquals("4093", m.get("vlan"));
		assertEquals(60545, m.get("srcPort"));
		assertEquals("151.200.99.22", m.get("dvc"));
		assertEquals("151.72.69.29", m.get("cncHost"));
		assertEquals("42053", m.get("externalId"));
		assertEquals("Mar 18 2014 10:03:29 Z", m.get("devTime"));
		assertEquals("89046625", m.get("sid"));
		assertEquals(80, m.get("cncPort"));
		assertEquals("https://Bora-FireEye./event_stream/events_for_bot?ev_id\\=42053", m.get("link"));
		assertEquals(80, m.get("dstPort"));
		assertEquals("GET /AppTag/TagCnt_xe2.asp HTTP/1.1::~~Host: ad.syndiapi.com::~~Accept: text/html, "
				+ "*/*::~~Accept-Encoding: identity::~~User-Agent: Mozilla/3.0 (compatible; Indy Library)::~~::~~",
				m.get("cncChannel"));
		assertEquals("10:04:96:52:de:96", m.get("srcMAC"));

	}
}
