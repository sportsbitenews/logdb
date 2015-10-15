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
package org.araqne.logparser.syslog.airtight;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class SpectraGuardLogParserTest {

	@Test
	public void testRogueClientLog() {
		String log = "<11:22:33:44:55:66>SpectraGuard Enterprise v6.2 : Start: Client_a [11:22:33:44:55:66] is active. : 1.2.3.4:2F : 2012-08-05T23:56:50+00:00 : High : 1987198 : 5 : 66 : 780";
		SpectraGuardLogParser p = new SpectraGuardLogParser();
		Map<String, Object> m = p.parse(line(log));

		assertEquals("11:22:33:44:55:66", m.get("sensor_mac"));
		assertEquals("SpectraGuard Enterprise v6.2", m.get("sensor_version"));
		assertEquals("Start", m.get("state"));
	}

	@Test
	public void testRogueApLog() {
		String log = "<11:22:33:44:55:66>SpectraGuard Enterprise v6.2 : Stop: ap_client [HOHO_WW] is active. : 1.2.3.4:10F : 2012-08-05T23:57:06+00:00 : High : 1987123 : 5 : 59 : 779";
		SpectraGuardLogParser p = new SpectraGuardLogParser();
		Map<String, Object> m = p.parse(line(log));

		assertEquals("11:22:33:44:55:66", m.get("sensor_mac"));
		assertEquals("SpectraGuard Enterprise v6.2", m.get("sensor_version"));
		assertEquals("Stop", m.get("state"));
	}

	@Test
	public void testRfSigAnomalyLog() {
		String log = "<11:22:33:44:55:66>SpectraGuard Enterprise v6.2 : Start: RF signature anomaly detected for Client [c] : 1.2.3.4:12F : 2012-08-08T04:45:25+00:00 : High : 2005417 : 5 : 65 : 502";
		SpectraGuardLogParser p = new SpectraGuardLogParser();
		Map<String, Object> m = p.parse(line(log));

		assertEquals("11:22:33:44:55:66", m.get("sensor_mac"));
		assertEquals("SpectraGuard Enterprise v6.2", m.get("sensor_version"));
	}

	@Test
	public void testDeauthFloodLog() {
		String log = "<11:22:33:44:55:66>SpectraGuard Enterprise v6.2 : Stop: Deauthentication flood attack is in progress against Authorized AP [55:66:77:88:99:00] and Client [amugae]. : 1.2.3.4:12F : 2012-08-06T00:16:35+00:00 : High : 1987420 : 5 : 52 : 255";
		SpectraGuardLogParser p = new SpectraGuardLogParser();
		Map<String, Object> m = p.parse(line(log));

		assertEquals("11:22:33:44:55:66", m.get("sensor_mac"));
		assertEquals("SpectraGuard Enterprise v6.2", m.get("sensor_version"));
		assertEquals("Stop", m.get("state"));
	}

	@Test
	public void testAdhocNetworkLog() {
		String log = "<11:22:33:44:55:66>SpectraGuard Enterprise v6.2 : Start: An Ad hoc network [amugae] involving one or more Authorized Clients is active. : 1.2.3.4://3f : 2012-08-08T04:39:47+00:00 : High : 2005377 : 5 : 61 : 791";
		SpectraGuardLogParser p = new SpectraGuardLogParser();
		Map<String, Object> m = p.parse(line(log));

		assertEquals("11:22:33:44:55:66", m.get("sensor_mac"));
		assertEquals("SpectraGuard Enterprise v6.2", m.get("sensor_version"));
		assertEquals("1.2.3.4", m.get("sensor_ip"));
		assertEquals("Start", m.get("state"));
		assertEquals("Ad Hoc", m.get("type"));
		assertEquals("amugae", m.get("adhoc"));
		assertEquals("//3f", m.get("location"));
		assertEquals("High", m.get("severity"));
	}

	@Test
	public void testFakeApLog() {
		String log = "<11:22:33:44:55:66>SpectraGuard Enterprise v6.2 : Use of Fake AP tool detected near Sensor [W] : 1.2.3.4:2F : 2012-08-05T23:56:50+00:00 : High : 1987200 : 5 : 52 : 299";
		SpectraGuardLogParser p = new SpectraGuardLogParser();
		Map<String, Object> m = p.parse(line(log));

		assertEquals("11:22:33:44:55:66", m.get("sensor_mac"));
		assertEquals("SpectraGuard Enterprise v6.2", m.get("sensor_version"));
	}

	@Test
	public void testIndeterminateApLog() {
		String log = "<11:22:33:44:55:66>SpectraGuard Enterprise v6.2 : Start: Indeterminate AP [WWW] is active. : 1.2.3.4://3f : 2012-08-05T23:56:57+00:00 : Medium : 1987201 : 5 : 59 : 281";
		SpectraGuardLogParser p = new SpectraGuardLogParser();
		Map<String, Object> m = p.parse(line(log));

		assertEquals("11:22:33:44:55:66", m.get("sensor_mac"));
		assertEquals("SpectraGuard Enterprise v6.2", m.get("sensor_version"));
		assertEquals("1.2.3.4", m.get("sensor_ip"));
		assertEquals("Start", m.get("state"));
	}

	@Test
	public void testUnauthorizedClientLog() {
		String log = "<11:22:33:44:55:66>SpectraGuard Enterprise v6.2 : Start: Unauthorized Client [55:66:77:88:99:00] is connected to Authorized AP. : 1.2.3.4://WHERE : 2012-08-05T23:57:04+00:00 : High : 1987203 : 5 : 66 : 796";
		SpectraGuardLogParser p = new SpectraGuardLogParser();
		Map<String, Object> m = p.parse(line(log));

		assertEquals("11:22:33:44:55:66", m.get("sensor_mac"));
		assertEquals("SpectraGuard Enterprise v6.2", m.get("sensor_version"));
		assertEquals("1.2.3.4", m.get("sensor_ip"));
		assertEquals("Start", m.get("state"));
		assertEquals("Misbehaving Client", m.get("type"));
		assertEquals("55:66:77:88:99:00", m.get("client"));
		assertEquals("//WHERE", m.get("location"));
		assertEquals("High", m.get("severity"));
	}

	@Test
	public void testAuthorizedApLog() {
		String log = "<11:22:33:44:55:66>SpectraGuard Enterprise v6.2 : Stop: Authorized AP [HOHO] is operating on non-allowed channel. : 1.2.3.4://3f : 2012-08-05T23:57:21+00:00 : Low : 1987173 : 5 : 51 : 515";
		SpectraGuardLogParser p = new SpectraGuardLogParser();
		Map<String, Object> m = p.parse(line(log));

		assertEquals("11:22:33:44:55:66", m.get("sensor_mac"));
		assertEquals("SpectraGuard Enterprise v6.2", m.get("sensor_version"));
		assertEquals("1.2.3.4", m.get("sensor_ip"));
		assertEquals("Stop", m.get("state"));
		assertEquals("Misconfigured AP", m.get("type"));
		assertEquals("HOHO", m.get("ap"));
		assertEquals("//3f", m.get("location"));
		assertEquals("Low", m.get("severity"));
	}

	@Test
	public void testAuthorizedClientLog() {
		String log = "<11:22:33:44:55:66>SpectraGuard Enterprise v6.2 : Start: Authorized Client [WERWER] is connected to a non-authorized AP. : 1.2.3.4://WHERE : 2012-08-06T00:22:10+00:00 : High : 1987468 : 5 : 66 : 799";
		SpectraGuardLogParser p = new SpectraGuardLogParser();
		Map<String, Object> m = p.parse(line(log));

		assertEquals("11:22:33:44:55:66", m.get("sensor_mac"));
		assertEquals("SpectraGuard Enterprise v6.2", m.get("sensor_version"));
		assertEquals("1.2.3.4", m.get("sensor_ip"));
		assertEquals("Start", m.get("state"));
		assertEquals("Misbehaving Client", m.get("type"));
		assertEquals("WERWER", m.get("client"));
		assertEquals("//WHERE", m.get("location"));
		assertEquals("High", m.get("severity"));
	}

	@Test
	public void testNetstumblerLog() {
		String log = "<11:22:33:44:55:66>SpectraGuard Enterprise v6.2 : Possible use of Netstumbler detected near Sensor [W] from Client [11:22:33:55:66:77] : 1.2.3.4://6F : 2012-08-05T23:58:26+00:00 : Medium : 1987228 : 5 : 53 : 268";
		SpectraGuardLogParser p = new SpectraGuardLogParser();
		Map<String, Object> m = p.parse(line(log));

		assertEquals("11:22:33:44:55:66", m.get("sensor_mac"));
		assertEquals("SpectraGuard Enterprise v6.2", m.get("sensor_version"));
		assertEquals("1.2.3.4", m.get("sensor_ip"));
		assertEquals("Scanning", m.get("type"));
		assertEquals("11:22:33:55:66:77", m.get("client"));
		assertEquals("//6F", m.get("location"));
		assertEquals("Medium", m.get("severity"));
	}

	@Test
	public void testApQuarantineLog() {
		String log = "<11:22:33:44:55:66>SpectraGuard Enterprise v6.2 : Start: AP [WER] needs to be quarantined. : 1.2.3.4:12F : 2012-08-06T00:02:03+00:00 : High : 1987269 : 5 : 69 : 831";
		SpectraGuardLogParser p = new SpectraGuardLogParser();
		Map<String, Object> m = p.parse(line(log));

		assertEquals("11:22:33:44:55:66", m.get("sensor_mac"));
		assertEquals("SpectraGuard Enterprise v6.2", m.get("sensor_version"));
	}

	@Test
	public void testClientQuarantineLog() {
		String log = "<11:22:33:44:55:66>SpectraGuard Enterprise v6.2 : Stop: Client [wer] needs to be quarantined. : 1.2.3.4://Foo : 2012-08-09T07:58:51+00:00 : High : 2014177 : 5 : 69 : 834";
		SpectraGuardLogParser p = new SpectraGuardLogParser();
		Map<String, Object> m = p.parse(line(log));

		assertEquals("11:22:33:44:55:66", m.get("sensor_mac"));
		assertEquals("SpectraGuard Enterprise v6.2", m.get("sensor_version"));
		assertEquals("1.2.3.4", m.get("sensor_ip"));
		assertEquals("Stop", m.get("state"));
		assertEquals("Prevention", m.get("type"));
		assertEquals("wer", m.get("client"));
		assertEquals("//Foo", m.get("location"));
		assertEquals("High", m.get("severity"));
	}

	@Test
	public void testHyundaiHeavyIndustryLogs() {
		String log = "Feb 26 16:22:46 1.2.3.4 <11:33:55:77:99:11>SpectraGuard Enterprise v6.5 : Stop: Client_a [wer] is active. : 1.2.3.4://where : 2013-02-26T07:42:11+00:00 : High : 584360 : 5 : 66 : 780: Closest Sensor [허허]";
		SpectraGuardLogParser p = new SpectraGuardLogParser();
		Map<String, Object> m = p.parse(line(log));

		assertEquals("1.2.3.4", m.get("host"));
		assertEquals("11:33:55:77:99:11", m.get("sensor_mac"));
		assertEquals("1.2.3.4", m.get("sensor_ip"));
		assertEquals("//where", m.get("location"));
		assertEquals("2013-02-26T07:42:11+00:00", m.get("date"));
		assertEquals("High", m.get("severity"));
		assertEquals("584360", m.get("event_id"));
		assertEquals("5", m.get("event_major_num"));
		assertEquals("66", m.get("event_intermediate_num"));
		assertEquals("780", m.get("event_minor_num"));
		assertEquals("Closest Sensor [허허]", m.get("closest_sensor_name"));
	}

	private Map<String, Object> line(String log) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", log);
		return m;
	}
}
