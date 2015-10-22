/*
 * Copyright 2015 Eediom Inc.
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
package org.araqne.logparser.krsyslog.secui;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.araqne.logparser.krsyslog.secui.MfiLogParser.Mode;
import org.junit.Test;

public class MfiLogParserTest {
	@Test
	public void testSample1() {
		String line = "<14>1 2012-12-03T14:57:14.065901Z [event] [192.168.1.201] 2012-12-03 14:56:34,2012-12-03 14:56:40,wer,test,profile1,PD#1(seg0),2604558950,37207985,탐지,1";

		MfiLogParser p = new MfiLogParser(Mode.CSV);
		Map<String, Object> m = p.parse(line(line));

		assertEquals("2012-12-03 14:56:34", m.get("start_time"));
		assertEquals("2012-12-03 14:56:40", m.get("end_time"));
		assertEquals("wer", m.get("machine"));
		assertEquals("test", m.get("rule_name"));
		assertEquals("profile1", m.get("profile"));
		assertEquals("PD#1(seg0)", m.get("pdomain"));
		assertEquals("2604558950", m.get("bytes"));
		assertEquals("37207985", m.get("packets"));
		assertEquals("탐지", m.get("action"));
		assertEquals("1", m.get("priority"));
	}

	@Test
	public void testSample2() {
		String line = "1 2015-09-25T15:27:28.025457Z [segment_traffic_rcv] [192.168.1.191] 2015-09-25 15:27:28,localhost,hoho#3,213,0,0,0,227,0,0,0,668563,0,0,0,1497356,0,0,0";

		MfiLogParser p = new MfiLogParser(Mode.CSV);
		Map<String, Object> m = p.parse(line(line));

		assertEquals("2015-09-25 15:27:28", m.get("timestamp"));
		assertEquals("localhost", m.get("machine"));
		assertEquals("hoho#3", m.get("segment"));
		assertEquals("213", m.get("in_pps_tot"));
		assertEquals("0", m.get("in_pps_detect"));
		assertEquals("0", m.get("in_pps_acl"));
		assertEquals("0", m.get("in_pps_system"));
		assertEquals("227", m.get("out_pps_tot"));
		assertEquals("0", m.get("out_pps_detect"));
		assertEquals("0", m.get("out_pps_acl"));
		assertEquals("0", m.get("out_pps_system"));
		assertEquals("668563", m.get("in_bps_tot"));
		assertEquals("0", m.get("in_bps_detect"));
		assertEquals("0", m.get("in_bps_acl"));
		assertEquals("0", m.get("in_bps_system"));
		assertEquals("1497356", m.get("out_bps_tot"));
		assertEquals("0", m.get("out_bps_detect"));
		assertEquals("0", m.get("out_bps_acl"));
		assertEquals("0", m.get("out_bps_system"));
	}

	private Map<String, Object> line(String line) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);
		return m;
	}
}
