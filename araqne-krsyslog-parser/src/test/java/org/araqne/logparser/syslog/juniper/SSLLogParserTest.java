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

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * @author kyun
 */
public class SSLLogParserTest {
	
	@Test
	public void testParser() {
		SslVpnLogParser parser = new SslVpnLogParser();
		Map<String, Object> m = parser
				.parse(line("Juniper: 2014-03-19 15:55:56 - SSLVPN_1 - [210.223.123.41] pp32462(Partner)[TOKTOK_NC, Common_NC, SKT_Common_NC] - "
						+ "Host Checker policy 'Cache Cleaner policy' passed on host 210.223.123.41 "
						+ "for user 'pp32462'."));
				
		assertEquals("Juniper", m.get("vendor"));
		assertEquals("SSLVPN_1", m.get("hostname"));
		
		Date time = (Date) m.get("logtime");
		Calendar c = Calendar.getInstance();
		c.setTime(time);
		assertEquals(2014, c.get(Calendar.YEAR));
		assertEquals(2, c.get(Calendar.MONTH));
		assertEquals(19, c.get(Calendar.DAY_OF_MONTH));
		assertEquals(15, c.get(Calendar.HOUR_OF_DAY));
		assertEquals(55, c.get(Calendar.MINUTE));
		assertEquals(56, c.get(Calendar.SECOND));
		
		assertEquals("210.223.123.41", m.get("ip"));
		assertEquals("pp32462(Partner)[TOKTOK_NC, Common_NC, SKT_Common_NC]", m.get("user"));
		assertEquals("Host Checker policy 'Cache Cleaner policy' passed on host 210.223.123.41 for user 'pp32462'.", m.get("msg"));
	}

	
	private Map<String, Object> line(String line) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);
		return m;
	}
}
