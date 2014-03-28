/*
 * Copyright 2012 Future Systems
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
package org.araqne.logparser.syslog.juniper;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class SrxLogParserTest {

	@Test
	public void testSessionCreateLog() {
		SrxLogParser parser = new SrxLogParser();
		Map<String, Object> m = parser
				.parse(line("May  8 02:52:01 RT_FLOW: RT_FLOW_SESSION_CREATE: session created 10.213.110.248/34613->72.14.203.188/5228 None 211.36.132.131/43335->72.14.203.188/5228 r1 None 6 TCP_1H trust untrust 80497606"));

		assertEquals("create", m.get("action"));
		assertEquals("10.213.110.248", m.get("src_ip"));
		assertEquals(34613, m.get("src_port"));
		assertEquals("72.14.203.188", m.get("dst_ip"));
		assertEquals(5228, m.get("dst_port"));
		assertEquals("211.36.132.131", m.get("nat_src_ip"));
		assertEquals(43335, m.get("nat_src_port"));
		assertEquals("72.14.203.188", m.get("nat_dst_ip"));
		assertEquals(5228, m.get("nat_dst_port"));
		assertEquals("r1", m.get("src_nat_rule"));
		assertEquals("None", m.get("dst_nat_rule"));
		assertEquals("6", m.get("protocol"));
		assertEquals("TCP_1H", m.get("policy"));
		assertEquals("trust", m.get("src_zone"));
		assertEquals("untrust", m.get("dst_zone"));
		assertEquals("80497606", m.get("session_id"));
	}

	@Test
	public void testTcpRstLog() {
		SrxLogParser parser = new SrxLogParser();
		Map<String, Object> m = parser
				.parse(line("May  8 02:52:02 RT_FLOW: RT_FLOW_SESSION_CLOSE: session closed TCP RST: 10.254.251.48/35639->72.14.203.188/5228 None 211.36.132.123/40488->72.14.203.188/5228 r1 None 6 TCP_1H trust untrust 80888035 46(4109) 42(4812) 13512"));

		assertEquals("close", m.get("action"));
		assertEquals("TCP RST", m.get("reason"));
		assertEquals("10.254.251.48", m.get("src_ip"));
		assertEquals(35639, m.get("src_port"));
		assertEquals("72.14.203.188", m.get("dst_ip"));
		assertEquals(5228, m.get("dst_port"));
		assertEquals("r1", m.get("src_nat_rule"));
		assertEquals("None", m.get("dst_nat_rule"));
		assertEquals("6", m.get("protocol"));
		assertEquals("TCP_1H", m.get("policy"));
		assertEquals("trust", m.get("src_zone"));
		assertEquals("untrust", m.get("dst_zone"));
		assertEquals("80888035", m.get("session_id"));
		assertEquals(46L, m.get("sent_pkts"));
		assertEquals(4109L, m.get("sent_bytes"));
		assertEquals(42L, m.get("rcvd_pkts"));
		assertEquals(4812L, m.get("rcvd_bytes"));
		assertEquals(13512L, m.get("elapsed_time"));
	}

	@Test
	public void testTcpFinLog() {
		SrxLogParser parser = new SrxLogParser();
		Map<String, Object> m = parser
				.parse(line("May  8 02:52:06 RT_FLOW: RT_FLOW_SESSION_CLOSE: session closed TCP FIN: 10.253.4.144/48577->74.125.71.188/5228 None 211.36.132.229/52104->74.125.71.188/5228 r1 None 6 TCP_1H trust untrust 100186027 0(0) 0(0) 1"));

		assertEquals("close", m.get("action"));
		assertEquals("TCP FIN", m.get("reason"));
		assertEquals("10.253.4.144", m.get("src_ip"));
		assertEquals(48577, m.get("src_port"));
		assertEquals("74.125.71.188", m.get("dst_ip"));
		assertEquals(5228, m.get("dst_port"));
		assertEquals("None", m.get("service"));
		assertEquals("r1", m.get("src_nat_rule"));
		assertEquals("None", m.get("dst_nat_rule"));
		assertEquals("6", m.get("protocol"));
		assertEquals("TCP_1H", m.get("policy"));
		assertEquals("trust", m.get("src_zone"));
		assertEquals("untrust", m.get("dst_zone"));
		assertEquals("100186027", m.get("session_id"));
		assertEquals(0L, m.get("sent_pkts"));
		assertEquals(0L, m.get("sent_bytes"));
		assertEquals(0L, m.get("rcvd_pkts"));
		assertEquals(0L, m.get("rcvd_bytes"));
		assertEquals(1L, m.get("elapsed_time"));
	}

	@Test
	public void testUnsetLog() {
		SrxLogParser parser = new SrxLogParser();
		Map<String, Object> m = parser
				.parse(line("May  8 02:52:02 RT_FLOW: RT_FLOW_SESSION_CLOSE: session closed unset: 10.253.92.74/58350->72.14.203.188/5228 None 211.36.132.81/53350->72.14.203.188/5228 r1 None 6 TCP_1H trust untrust 80038716 58(5362) 64(6924) 28150"));

		assertEquals("close", m.get("action"));
		assertEquals("unset", m.get("reason"));
		assertEquals("10.253.92.74", m.get("src_ip"));
		assertEquals(58350, m.get("src_port"));
		assertEquals("72.14.203.188", m.get("dst_ip"));
		assertEquals(5228, m.get("dst_port"));
		assertEquals("None", m.get("service"));
		assertEquals("r1", m.get("src_nat_rule"));
		assertEquals("None", m.get("dst_nat_rule"));
		assertEquals("6", m.get("protocol"));
		assertEquals("TCP_1H", m.get("policy"));
		assertEquals("trust", m.get("src_zone"));
		assertEquals("untrust", m.get("dst_zone"));
		assertEquals("80038716", m.get("session_id"));
		assertEquals(58L, m.get("sent_pkts"));
		assertEquals(5362L, m.get("sent_bytes"));
		assertEquals(64L, m.get("rcvd_pkts"));
		assertEquals(6924L, m.get("rcvd_bytes"));
		assertEquals(28150L, m.get("elapsed_time"));
	}

	@Test
	public void testDenyLog() {
		SrxLogParser parser = new SrxLogParser();
		Map<String, Object> m = parser
				.parse(line("Dec 16 05:02:28  RT_FLOW: RT_FLOW_SESSION_DENY: session denied 10.0.0.32/9370->63.251.254.131/370 None 17(0) default-permit trust untrust"));

		assertEquals("deny", m.get("action"));
		assertEquals("10.0.0.32", m.get("src_ip"));
		assertEquals(9370, m.get("src_port"));
		assertEquals("63.251.254.131", m.get("dst_ip"));
		assertEquals(370, m.get("dst_port"));
		assertEquals("None", m.get("service"));
		assertEquals("17", m.get("protocol"));
		assertEquals("0", m.get("icmp_type"));
		assertEquals("default-permit", m.get("policy"));
		assertEquals("trust", m.get("src_zone"));
		assertEquals("untrust", m.get("dst_zone"));
	}

	@Test
	public void testCreateLog() {
		SrxLogParser parser = new SrxLogParser();
		Map<String, Object> m = parser
				.parse(line("1 2014-01-19T11:16:39.616 SRX3400_1 RT_FLOW - RT_FLOW_SESSION_CREATE [junos@2636.1.1.1.2.35 source-address=\"226.57.142.105\" source-port=\"46556\" destination-address=\"132.58.192.215\" destination-port=\"1152\" service-name=\"None\" nat-source-address=\"226.57.142.105\" nat-source-port=\"46556\" nat-destination-address=\"10.201.50.215\" nat-destination-port=\"1152\" src-nat-rule-name=\"None\" dst-nat-rule-name=\"natrule3\" protocol-id=\"6\" policy-name=\"268\" source-zone-name=\"untrust\" destination-zone-name=\"trust\" session-id-32=\"60663108\" username=\"N/A\" roles=\"N/A\" packet-incoming-interface=\"eth0\"]"));

		assertEquals("create", m.get("action"));
		assertEquals("226.57.142.105", m.get("source-address"));
		assertEquals("46556", m.get("source-port"));
		assertEquals("132.58.192.215", m.get("destination-address"));
		assertEquals("1152", m.get("destination-port"));
		assertEquals("None", m.get("service-name"));
		assertEquals("226.57.142.105", m.get("nat-source-address"));
		assertEquals("46556", m.get("nat-source-port"));
		assertEquals("10.201.50.215", m.get("nat-destination-address"));
		assertEquals("1152", m.get("nat-destination-port"));
		assertEquals("None", m.get("src-nat-rule-name"));
		assertEquals("natrule3", m.get("dst-nat-rule-name"));
		assertEquals("6", m.get("protocol-id"));
		assertEquals("268", m.get("policy-name"));
		assertEquals("untrust", m.get("source-zone-name"));
		assertEquals("trust", m.get("destination-zone-name"));
		assertEquals("60663108", m.get("session-id-32"));
		assertEquals("N/A", m.get("username"));
		assertEquals("N/A", m.get("roles"));
		assertEquals("eth0", m.get("packet-incoming-interface"));
	}

	@Test
	public void testCloseLog() {
		SrxLogParser parser = new SrxLogParser();
		Map<String, Object> m = parser
				.parse(line("1 2014-01-19T11:16:37.266 SRX3400_1 RT_FLOW - RT_FLOW_SESSION_CLOSE [junos@2636.1.1.1.2.35 reason=\"ICMP error\" source-address=\"10.40.29.180\" source-port=\"56255\" destination-address=\"216.85.144.3\" destination-port=\"53\" service-name=\"junos-dns-udp\" nat-source-address=\"132.58.192.15\" nat-source-port=\"57572\" nat-destination-address=\"216.85.144.3\" nat-destination-port=\"53\" src-nat-rule-name=\"natrule7\" dst-nat-rule-name=\"None\" protocol-id=\"17\" policy-name=\"1000\" source-zone-name=\"trust\" destination-zone-name=\"untrust\" session-id-32=\"40174772\" packets-from-client=\"1\" bytes-from-client=\"75\" packets-from-server=\"0\" bytes-from-server=\"0\" elapsed-time=\"1\" application=\"UNKNOWN\" nested-application=\"UNKNOWN\" username=\"N/A\" roles=\"N/A\" packet-incoming-nterface=\"eth1\"]"));

		assertEquals("close", m.get("action"));
		assertEquals("ICMP error", m.get("reason"));
		assertEquals("10.40.29.180", m.get("source-address"));
		assertEquals("56255", m.get("source-port"));
		assertEquals("216.85.144.3", m.get("destination-address"));
		assertEquals("53", m.get("destination-port"));
		assertEquals("junos-dns-udp", m.get("service-name"));
		assertEquals("132.58.192.15", m.get("nat-source-address"));
		assertEquals("57572", m.get("nat-source-port"));
		assertEquals("216.85.144.3", m.get("nat-destination-address"));
		assertEquals("53", m.get("nat-destination-port"));
		assertEquals("natrule7", m.get("src-nat-rule-name"));
		assertEquals("None", m.get("dst-nat-rule-name"));
		assertEquals("17", m.get("protocol-id"));
		assertEquals("1000", m.get("policy-name"));
		assertEquals("trust", m.get("source-zone-name"));
		assertEquals("untrust", m.get("destination-zone-name"));
		assertEquals("40174772", m.get("session-id-32"));
		assertEquals("1", m.get("packets-from-client"));
		assertEquals("75", m.get("bytes-from-client"));
		assertEquals("0", m.get("packets-from-server"));
		assertEquals("0", m.get("bytes-from-server"));
		assertEquals("1", m.get("elapsed-time"));
		assertEquals("UNKNOWN", m.get("application"));
		assertEquals("UNKNOWN", m.get("nested-application"));
		assertEquals("N/A", m.get("username"));
		assertEquals("N/A", m.get("roles"));
		assertEquals("eth1", m.get("packet-incoming-nterface"));
	}

	@Test
	public void testDenyLog2() {
		SrxLogParser parser = new SrxLogParser();
		Map<String, Object> m = parser
				.parse(line("1 2014-01-19T11:16:37.375 SRX3400_1 RT_FLOW - RT_FLOW_SESSION_DENY [junos@2636.1.1.1.2.35 source-address=\"230.178.7.14\" source-port=\"39722\" destination-address=\"132.58.173.32\" destination-port=\"1931\" service-name=\"None\" protocol-id=\"6\" icmp-type=\"0\" policy-name=\"2000\" source-zone-name=\"untrust\" destination-zone-name=\"trust\" application=\"UNKNOWN\" nested-application=\"UNKNOWN\" username=\"N/A\" roles=\"N/A\" packet-incoming-interface=\"eth0\"]"));

		assertEquals("deny", m.get("action"));
		assertEquals("230.178.7.14", m.get("source-address"));
		assertEquals("39722", m.get("source-port"));
		assertEquals("132.58.173.32", m.get("destination-address"));
		assertEquals("1931", m.get("destination-port"));
		assertEquals("None", m.get("service-name"));
		assertEquals("6", m.get("protocol-id"));
		assertEquals("0", m.get("icmp-type"));
		assertEquals("2000", m.get("policy-name"));
		assertEquals("untrust", m.get("source-zone-name"));
		assertEquals("trust", m.get("destination-zone-name"));
		assertEquals("UNKNOWN", m.get("application"));
		assertEquals("UNKNOWN", m.get("nested-application"));
		assertEquals("N/A", m.get("username"));
		assertEquals("N/A", m.get("roles"));
		assertEquals("eth0", m.get("packet-incoming-interface"));
	}

	private Map<String, Object> line(String line) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);
		return m;
	}
}
