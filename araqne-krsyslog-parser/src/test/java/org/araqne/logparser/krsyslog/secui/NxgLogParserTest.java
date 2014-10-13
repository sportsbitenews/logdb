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

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

import org.araqne.logparser.krsyslog.secui.NxgLogParser;
import org.junit.Test;

public class NxgLogParserTest {
	@Test
	public void testPocSample1() {
		String log = "<398>[LOG_AUDIT] 높음 	2013-05-14 14:39:38 	secui 	130.1.185.24 	130.1.254.197 	130.1.254.198 	HA SYNC 	쉘명령 	- 	/fw/bin/do_jni_action LOG 	적용 	성공 ";
		NxgLogParser p = new NxgLogParser();
		Map<String, Object> m = p.parse(line(log));

		assertEquals("audit", m.get("type"));
		assertEquals("높음", m.get("level"));
		assertEquals(date(2013, 5, 14, 14, 39, 38), m.get("_time"));
		assertEquals("secui 	130.1.185.24 	130.1.254.197 	130.1.254.198 	HA SYNC 	쉘명령 	- 	/fw/bin/do_jni_action LOG 	적용 	성공 ",
				m.get("msg"));
	}

	@Test
	public void testPocSample2() {
		String log = "<398>[LOG_AUDIT] 높음 	2013-05-14 14:39:37 	secui 	130.1.185.24 	130.1.254.197 	로그/리포트 	로그/경고 	로그 적용 	- 	-	적용 	성공 ";
		NxgLogParser p = new NxgLogParser();
		Map<String, Object> m = p.parse(line(log));

		assertEquals(date(2013, 5, 14, 14, 39, 37), m.get("_time"));
		assertEquals("audit", m.get("type"));
		assertEquals("높음", m.get("level"));
		assertEquals(date(2013, 5, 14, 14, 39, 37), m.get("_time"));
		assertEquals("secui 	130.1.185.24 	130.1.254.197 	로그/리포트 	로그/경고 	로그 적용 	- 	-	적용 	성공 ", m.get("msg"));
	}

	@Test
	public void testPocSample3() {
		String log = "<206>[LOG_ADMITTED] 2013-05-14 14:40:12,1966,211.181.254.201,0,0/icmp,130.1.194.69,0,78,0,0,External";
		NxgLogParser p = new NxgLogParser();
		Map<String, Object> m = p.parse(line(log));

		assertEquals(date(2013, 5, 14, 14, 40, 12), m.get("_time"));
		assertEquals("allow", m.get("type"));
		assertEquals("0/icmp", m.get("protocol"));
		assertEquals("211.181.254.201", m.get("src_ip"));
		assertEquals("130.1.194.69", m.get("dst_ip"));
		assertEquals("External", m.get("zone"));
	}

	@Test
	public void testPocSample4() {
		String log = "<214>[LOG_DENIED] 2013-05-14 14:39:37,1700,130.1.111.210,211.181.255.1,3/icmp,0,3,DENY,1,Internal";
		NxgLogParser p = new NxgLogParser();
		Map<String, Object> m = p.parse(line(log));

		assertEquals("deny", m.get("type"));
		assertEquals(date(2013, 5, 14, 14, 39, 37), m.get("_time"));
		assertEquals("130.1.111.210", m.get("src_ip"));
		assertEquals("211.181.255.1", m.get("dst_ip"));
		assertEquals("3/icmp", m.get("protocol"));
		assertEquals("Internal", m.get("zone"));
	}

	@Test
	public void testPocSample5() {
		String log = "<214>[LOG_DENIED] 2013-05-14 14:40:13,1700,130.1.101.78,211.115.106.203,80/tcp,54576,80,DENY,1,Internal";
		NxgLogParser p = new NxgLogParser();
		Map<String, Object> m = p.parse(line(log));

		assertEquals(date(2013, 5, 14, 14, 40, 13), m.get("_time"));
		assertEquals("deny", m.get("type"));
		assertEquals("80/tcp", m.get("protocol"));
		assertEquals("130.1.101.78", m.get("src_ip"));
		assertEquals("211.115.106.203", m.get("dst_ip"));
		assertEquals(54576, m.get("src_port"));
		assertEquals(80, m.get("dst_port"));
		assertEquals("Internal", m.get("zone"));
	}

	@Test
	public void testPocSample6() {
		String log = "<382>[LOG_NAT] 2013-05-14 14:40:12,130.1.181.16,55526,211.181.253.38,55526,55526/udp,Outbound";
		NxgLogParser p = new NxgLogParser();
		Map<String, Object> m = p.parse(line(log));

		assertEquals("nat", m.get("type"));
		assertEquals("55526/udp", m.get("protocol"));
		assertEquals("Outbound", m.get("direction"));
		assertEquals("130.1.181.16", m.get("src_ip"));
		assertEquals(55526, m.get("src_port"));
		assertEquals("211.181.253.38", m.get("nat_src_ip"));
		assertEquals(55526, m.get("nat_src_port"));
	}

	private Date date(int year, int mon, int day, int hour, int min, int sec) {
		Calendar c = Calendar.getInstance();
		c.set(Calendar.YEAR, 2013);
		c.set(Calendar.MONTH, mon - 1);
		c.set(Calendar.DAY_OF_MONTH, day);
		c.set(Calendar.HOUR_OF_DAY, hour);
		c.set(Calendar.MINUTE, min);
		c.set(Calendar.SECOND, sec);
		c.set(Calendar.MILLISECOND, 0);
		return c.getTime();
	}

	private Map<String, Object> line(String log) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", log);
		return m;
	}
}
