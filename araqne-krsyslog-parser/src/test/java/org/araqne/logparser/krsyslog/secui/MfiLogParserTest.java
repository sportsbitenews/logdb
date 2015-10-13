/*
 * Copyright 2013 Eediom Inc.
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

		assertEquals("192.168.1.201", m.get("from_ip"));
		assertEquals("2012-12-03 14:56:34", m.get("start_time"));
	}

	@Test
	public void testSample2() {
		String line = "1 2015-09-25T15:27:28.025457Z [segment_traffic_rcv] [192.168.1.191] 2015-09-25 15:27:28,localhost,hoho#3,213,0,0,0,227,0,0,0,668563,0,0,0,1497356,0,0,0";

		MfiLogParser p = new MfiLogParser(Mode.CSV);
		Map<String, Object> m = p.parse(line(line));

		assertEquals("segment_traffic_rcv", m.get("log_type"));
		assertEquals("1497356", m.get("out_bps_tot"));
	}

	private Map<String, Object> line(String line) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);
		return m;
	}
}
