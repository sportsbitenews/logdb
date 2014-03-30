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
package org.araqne.logparser.syslog.riorey;

import static org.junit.Assert.assertEquals;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * @author kyun
 */
public class RioreyDdosLogParserTest {

	@Test
	public void testParser() {
		String line = "175.223.12.223, 0, 0.0.0.0, 0, TCP_SESSION, 2014-3-19 6:44:31, 2014-3-19 17:9:48, 0, , TCP, A, AFA, Agent Filtered IP: Dropped packets: 0 Dropped bytes: 0";
		RioreyDdosLogParser parser = new RioreyDdosLogParser();

		Map<String, Object> m = parser.parse(line(line));

		assertEquals("175.223.12.223", m.get("attacker_ip"));
		assertEquals(0, m.get("attacker_port"));
		assertEquals("0.0.0.0", m.get("victim_ip"));
		assertEquals(0, m.get("victim_port"));
		assertEquals("TCP_SESSION", m.get("protocol_info"));

		Date time = (Date) m.get("start_time");
		Calendar c = Calendar.getInstance();
		c.setTime(time);
		assertEquals(2014, c.get(Calendar.YEAR));
		assertEquals(2, c.get(Calendar.MONTH));
		assertEquals(19, c.get(Calendar.DAY_OF_MONTH));
		assertEquals(6, c.get(Calendar.HOUR_OF_DAY));
		assertEquals(44, c.get(Calendar.MINUTE));
		assertEquals(31, c.get(Calendar.SECOND));

		time = (Date) m.get("expiry_time");
		c.setTime(time);
		assertEquals(2014, c.get(Calendar.YEAR));
		assertEquals(2, c.get(Calendar.MONTH));
		assertEquals(19, c.get(Calendar.DAY_OF_MONTH));
		assertEquals(17, c.get(Calendar.HOUR_OF_DAY));
		assertEquals(9, c.get(Calendar.MINUTE));
		assertEquals(48, c.get(Calendar.SECOND));

		assertEquals(0, m.get("packet_length"));
		assertEquals(null, m.get("fragment_offset"));
		assertEquals("TCP", m.get("protocol"));
		assertEquals("A", m.get("tcp_flags"));
		assertEquals("AFA", m.get("packet_status"));
		assertEquals("Agent Filtered IP: Dropped packets: 0 Dropped bytes: 0", m.get("extra_info"));

	}

	private Map<String, Object> line(String line) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);
		return m;
	}
}
