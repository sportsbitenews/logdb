/**
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
package org.araqne.logparser.syslog.symantec;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * @author kyun
 */
public class SwgLogParserTest {
	@Test
	public void testParser() {
		String line = "Symantec Web Gateway Alert: [Alert Name - Infection_Minor] (Count: 1), (Host: 10.200.166.179), (Detection Type: 1), (Threat Name: Active Bot),"
				+ " (Threat Category: Botnet), (Severity: 3), (Threat Description: The computer is a Bot, an active member of a Botnet. It communicated with Botnet Control (C&C) and has actively engaged in at least one other Bot Activity such as IP Scanning, Spamming, or DDoS (Distributed Denial of Service).)";
		SwgLogParser parser = new SwgLogParser();
		Map<String, Object> m = parser.parse(line(line));

		assertEquals(1, m.get("count"));
		assertEquals("10.200.166.179", m.get("host"));
		assertEquals(1, m.get("detection_type"));
		assertEquals("Active Bot", m.get("threat_name"));
		assertEquals("Botnet", m.get("threat_category"));
		assertEquals(3, m.get("severity"));
		assertEquals(
				"The computer is a Bot, an active member of a Botnet. It communicated with Botnet Control (C&C) and has actively engaged in at least one other Bot Activity such as IP Scanning, Spamming, or DDoS (Distributed Denial of Service).",
				m.get("threat_description"));
	}

	private Map<String, Object> line(String line) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);
		return m;
	}
}
