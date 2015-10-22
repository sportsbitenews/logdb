/*
 * Copyright 2014 Eediom Inc
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

public class SrxNewLogParserTest {
	@Test
	public void testSessionCreateLog() {
		SrxNewLogParser parser = new SrxNewLogParser();
		Map<String, Object> m = parser
				.parse(line("1 2014-03-19T06:15:38.899 IS_8F_DEV_FW_M RT_FLOW - RT_FLOW_SESSION_CREATE [junos@2636.1.1.1.2.49 "
						+ "source-address=\"172.19.112.110\" source-port=\"45233\" destination-address=\"10.10.100.39\" "
						+ "destination-port=\"10051\" service-name=\"None\" nat-source-address=\"172.19.112.110\" "
						+ "nat-source-port=\"45233\" nat-destination-address=\"10.10.100.39\" nat-destination-port=\"10051\" "
						+ "src-nat-rule-name=\"None\" dst-nat-rule-name=\"None\" protocol-id=\"6\" policy-name=\"38\" "
						+ "source-zone-name=\"DMZ\" destination-zone-name=\"Trust\" session-id-32=\"20152526\" "
						+ "username=\"N/A\" roles=\"N/A\" " + "packet-incoming-interface=\"reth2.0\"]"));

		assertEquals("2014-03-19T06:15:38.899", m.get("start_time"));
		assertEquals("IS_8F_DEV_FW_M", m.get("device_id"));
		assertEquals("RT_FLOW_SESSION_CREATE", m.get("action"));

		assertEquals("172.19.112.110", m.get("source-address"));
		assertEquals("45233", m.get("source-port"));
		assertEquals("10.10.100.39", m.get("destination-address"));
		assertEquals("10051", m.get("destination-port"));
		assertEquals("None", m.get("service-name"));
		assertEquals("172.19.112.110", m.get("nat-source-address"));
		assertEquals("45233", m.get("nat-source-port"));
		assertEquals("10.10.100.39", m.get("nat-destination-address"));
		assertEquals("10051", m.get("nat-destination-port"));
		assertEquals("None", m.get("src-nat-rule-name"));
		assertEquals("None", m.get("dst-nat-rule-name"));
		assertEquals("6", m.get("protocol-id"));
		assertEquals("38", m.get("policy-name"));
		assertEquals("DMZ", m.get("source-zone-name"));
		assertEquals("Trust", m.get("destination-zone-name"));
		assertEquals("20152526", m.get("session-id-32"));
		assertEquals("N/A", m.get("username"));
		assertEquals("N/A", m.get("roles"));
		assertEquals("reth2.0", m.get("packet-incoming-interface"));
	}

	@Test
	public void testSessionDenyLog() {
		SrxNewLogParser parser = new SrxNewLogParser();
		Map<String, Object> m = parser
				.parse(line("1 2015-09-12T00:00:01.363+09:00 INTERNET_SRX3400_1 RT_FLOW - RT_FLOW_SESSION_DENY [junos@2636.1.1.1.2.35 source-address=\"54.24.147.183\" source-port=\"7002\" destination-address=\"124.166.84.17\" destination-port=\"80\" service-name=\"junos-http\" protocol-id=\"6\" icmp-type=\"0\" policy-name=\"2000\" source-zone-name=\"untrust\" destination-zone-name=\"trust\" application=\"UNKNOWN\" nested-application=\"UNKNOWN\" username=\"N/A\" roles=\"N/A\" packet-incoming-interface=\"reth0.0\" encrypted=\"UNKNOWN\" reason=\"policy deny\"]"));

		assertEquals("2015-09-12T00:00:01.363+09:00", m.get("start_time"));
		assertEquals("INTERNET_SRX3400_1", m.get("device_id"));
		assertEquals("RT_FLOW_SESSION_DENY", m.get("action"));

		assertEquals("54.24.147.183", m.get("source-address"));
		assertEquals("7002", m.get("source-port"));
	}

	@Test
	public void testSessionCloseLog() {
		SrxNewLogParser parser = new SrxNewLogParser();
		Map<String, Object> m = parser
				.parse(line("1 2015-09-11T23:59:58.964+09:00 INTERNET_SRX3400_1 RT_FLOW - RT_FLOW_SESSION_CLOSE [junos@2636.1.1.1.2.35 reason=\"idle Timeout\" source-address=\"10.20.57.88\" source-port=\"56908\" destination-address=\"211.15.106.204\" destination-port=\"80\" service-name=\"junos-http\" nat-source-address=\"124.66.84.4\" nat-source-port=\"41736\" nat-destination-address=\"211.115.106.204\" nat-destination-port=\"80\" src-nat-rule-name=\"rule_1\" dst-nat-rule-name=\"None\" protocol-id=\"6\" policy-name=\"1000\" source-zone-name=\"trust\" destination-zone-name=\"untrust\" session-id-32=\"60884892\" packets-from-client=\"5\" bytes-from-client=\"561\" packets-from-server=\"4\" bytes-from-server=\"362\" elapsed-time=\"305\" application=\"UNKNOWN\" nested-application=\"UNKNOWN\" username=\"N/A\" roles=\"N/A\" packet-incoming-interface=\"reth1.0\" encrypted=\"UNKNOWN\"]"));

		assertEquals("2015-09-11T23:59:58.964+09:00", m.get("start_time"));
		assertEquals("INTERNET_SRX3400_1", m.get("device_id"));
		assertEquals("RT_FLOW_SESSION_CLOSE", m.get("action"));

		assertEquals("124.66.84.4", m.get("nat-source-address"));
		assertEquals("UNKNOWN", m.get("encrypted"));
	}

	@Test
	public void testBindingDeleteLog() {
		SrxNewLogParser parser = new SrxNewLogParser();
		Map<String, Object> m = parser
				.parse(line("1 2015-09-12T00:12:21.203+09:00 INTERNET_SRX3400_1 RT_NAT - RT_PST_NAT_BINDING_DELETE [junos@2636.1.1.1.2.35 state=\"Query    \" logical-system-id=\"0\" internal-ip=\"10.202.75.25\" internal-port=\"5060\" internal-protocol=\"17\" reflexive-ip=\"24.66.184.4\" reflexive-port=\"42661\" reflexive-protocol=\"17\"]"));

		assertEquals("2015-09-12T00:12:21.203+09:00", m.get("start_time"));
		assertEquals("INTERNET_SRX3400_1", m.get("device_id"));
		assertEquals("RT_PST_NAT_BINDING_DELETE", m.get("action"));

		assertEquals("10.202.75.25", m.get("internal-ip"));
		assertEquals("17", m.get("reflexive-protocol"));
	}

	@Test
	public void testBindingUpdateLog() {
		SrxNewLogParser parser = new SrxNewLogParser();
		Map<String, Object> m = parser
				.parse(line("1 2015-09-12T00:12:46.796+09:00 INTERNET_SRX3400_1 RT_NAT - RT_PST_NAT_BINDING_UPDATE [junos@2636.1.1.1.2.35]"));

		assertEquals("2015-09-12T00:12:46.796+09:00", m.get("start_time"));
		assertEquals("INTERNET_SRX3400_1", m.get("device_id"));
		assertEquals("RT_PST_NAT_BINDING_UPDATE", m.get("action"));
	}

	private Map<String, Object> line(String line) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);
		return m;
	}
}
