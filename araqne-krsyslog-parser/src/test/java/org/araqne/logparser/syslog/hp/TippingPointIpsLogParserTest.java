/*
 * Copyright 2014 Eediom Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.araqne.logparser.syslog.hp;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.araqne.logparser.syslog.hp.TippingPointIpsLogParser.Mode;
import org.junit.Test;

/**
 * @author kyun
 */
public class TippingPointIpsLogParserTest {

	@Test
	public void testParser() {
		String line = "Mar 18 19:12:23 sms-server 8	1	591aafad-b9f6-11e1-7265-be812d7c8911	00000001-0001-0001-0001-000000007121	7121: TCP: Header Length Invalid, e.g., Fragroute	7121	ip	113.216.83.239	35385	203.235.200.42	80	1	3	1	MCIPS_1	100739839	1395137543039	4301345";
		TippingPointIpsLogParser parser = new TippingPointIpsLogParser(Mode.TSV);
		Map<String, Object> m = parser.parse(line(line));

		assertEquals("Mar 18 19:12:23", m.get("time"));
		assertEquals("sms-server", m.get("hostname"));
		assertEquals(8, m.get("action"));
		assertEquals(1, m.get("serverity"));
		assertEquals("591aafad-b9f6-11e1-7265-be812d7c8911", m.get("policy_uuid"));
		assertEquals("00000001-0001-0001-0001-000000007121", m.get("sig_uuid"));
		assertEquals("7121: TCP: Header Length Invalid, e.g., Fragroute", m.get("sig_name"));
		assertEquals(7121, m.get("sig_no"));
		assertEquals("ip", m.get("protocol"));
		assertEquals("113.216.83.239", m.get("src_ip"));
		assertEquals(35385, m.get("src_port"));
		assertEquals("203.235.200.42", m.get("dst_ip"));
		assertEquals(80, m.get("dst_port"));
		assertEquals(1, m.get("hit"));
		assertEquals("3", m.get("src_zone"));
		assertEquals("1", m.get("dst_zone"));
		assertEquals("MCIPS_1", m.get("device_name"));
		assertEquals("100739839", m.get("taxonomy_id"));
		assertEquals("1395137543039", m.get("event_timestamp"));
		assertEquals("4301345", m.get("comments"));
		assertEquals(null, m.get("event_seqno"));
	}
	
	
	@Test
	public void testParser2() {
		String line =  "Apr  9 13:21:01 sms-server 7	2	92136183-4807-11e1-7265-be812d7c8911	00000001-0001-0001-0001-000000011984	11984: HTTP: Apache Default Configuration Information Disclosure Vulnerability	11984	http	211.177.193.211	34584	203.235.206.185	80	1	3	1	MCIPS_1	17107713	1397017261042	4346393";
		
		TippingPointIpsLogParser parser = new TippingPointIpsLogParser(Mode.TSV);
		Map<String, Object> m = parser.parse(line(line));

		assertEquals("Apr  9 13:21:01", m.get("time"));
		assertEquals("sms-server", m.get("hostname"));
		assertEquals(7, m.get("action"));
		assertEquals(2, m.get("serverity"));
		assertEquals("92136183-4807-11e1-7265-be812d7c8911", m.get("policy_uuid"));
		assertEquals("00000001-0001-0001-0001-000000011984", m.get("sig_uuid"));
		assertEquals("11984: HTTP: Apache Default Configuration Information Disclosure Vulnerability", m.get("sig_name"));
		assertEquals(11984, m.get("sig_no"));
		assertEquals("http", m.get("protocol"));
		assertEquals("211.177.193.211", m.get("src_ip"));
		assertEquals(34584, m.get("src_port"));
		assertEquals("203.235.206.185", m.get("dst_ip"));
		assertEquals(80, m.get("dst_port"));
		assertEquals(1, m.get("hit"));
		assertEquals("3", m.get("src_zone"));
		assertEquals("1", m.get("dst_zone"));
		assertEquals("MCIPS_1", m.get("device_name"));
		assertEquals("17107713", m.get("taxonomy_id"));
		assertEquals("1397017261042", m.get("event_timestamp"));
		assertEquals("4346393", m.get("comments"));
		assertEquals(null, m.get("event_seqno"));
	}
	
	@Test
	public void testParser3() {
		String line = "Apr 14 17:38:21 KCCSec-SMS 8,2,5ac7ae11-52a2-48ab-a91a-f2afffe9286d,00000001-0001-0001-0001-000000000291,\"0291: Invalid TCP Traffic: Possible Recon Scan (FIN no ACK)\",291,\"tcp\",115.20.153.220,61370,112.175.22.114,80,1,3,6,\"Mezzomedia_660\",100741885,1429000701086,1886330";
		
		TippingPointIpsLogParser parser = new TippingPointIpsLogParser(Mode.CSV);
		Map<String, Object> m = parser.parse(line(line));

		assertEquals("Apr 14 17:38:21", m.get("time"));
		assertEquals("KCCSec-SMS", m.get("hostname"));
		assertEquals(8, m.get("action"));
		assertEquals(2, m.get("serverity"));
		assertEquals("5ac7ae11-52a2-48ab-a91a-f2afffe9286d", m.get("policy_uuid"));
		assertEquals("00000001-0001-0001-0001-000000000291", m.get("sig_uuid"));
		assertEquals("0291: Invalid TCP Traffic: Possible Recon Scan (FIN no ACK)", m.get("sig_name"));
		assertEquals(291, m.get("sig_no"));
		assertEquals("tcp", m.get("protocol"));
		assertEquals("115.20.153.220", m.get("src_ip"));
		assertEquals(61370, m.get("src_port"));
		assertEquals("112.175.22.114", m.get("dst_ip"));
		assertEquals(80, m.get("dst_port"));
		assertEquals(1, m.get("hit"));
		assertEquals("3", m.get("src_zone"));
		assertEquals("6", m.get("dst_zone"));
		assertEquals("Mezzomedia_660", m.get("device_name"));
		assertEquals("100741885", m.get("taxonomy_id"));
		assertEquals("1429000701086", m.get("event_timestamp"));
		assertEquals("1886330", m.get("comments"));
		assertEquals(null, m.get("event_seqno"));
	}
	
	@Test
	public void testParser4() {
		String line = "Apr 16 13:16:45 KCCSec-SMS 8,1,e2da08ff-1ecf-4d5b-b5f2-497facbdb2a8,00000001-0001-0001-0001-000000007104,\"7104: IP: Duplicate Fragment, e.g., Fragroute\",7104,\"ip\",211.219.146.47,0,112.175.22.113,0,1,3,6,\"Mezzomedia_660\",100741887,1429157805067,1936372";
		
		TippingPointIpsLogParser parser = new TippingPointIpsLogParser(Mode.CSV);
		Map<String, Object> m = parser.parse(line(line));

		assertEquals("Apr 16 13:16:45", m.get("time"));
		assertEquals("KCCSec-SMS", m.get("hostname"));
		assertEquals(8, m.get("action"));
		assertEquals(1, m.get("serverity"));
		assertEquals("e2da08ff-1ecf-4d5b-b5f2-497facbdb2a8", m.get("policy_uuid"));
		assertEquals("00000001-0001-0001-0001-000000007104", m.get("sig_uuid"));
		assertEquals("7104: IP: Duplicate Fragment  e.g.  Fragroute", m.get("sig_name"));
	}
	
	private Map<String, Object> line(String line) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);
		return m;
	}
}
