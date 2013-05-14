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
package org.araqne.logparser.syslog.winstechnet;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import org.junit.Test;

public class SniperLogParserTest {
	@Test
	public void testPocSample1() {
		String log = "<36>[SNIPER-2000] [Attack_Name=(1181)MS WINS Server Registration Spoofing Vuln-2[Req](UDP-137)], "
				+ "[Time=2013/05/14 14:26:27], [Hacker=130.1.11.115], [Victim=130.1.11.255], [Protocol=udp/137], "
				+ "[Risk=High], [Handling=Alarm], [Information=], [SrcPort=137]";
		SniperLogParser p = new SniperLogParser();
		Map<String, Object> m = p.parse(line(log));

		assertEquals(date(2013, 5, 14, 14, 26, 27), m.get("_time"));
		assertEquals("(1181)MS WINS Server Registration Spoofing Vuln-2[Req](UDP-137)", m.get("attack_name"));
		assertEquals("130.1.11.115", m.get("hacker"));
		assertEquals("130.1.11.255", m.get("victim"));
		assertEquals("udp/137", m.get("protocol"));
		assertEquals("High", m.get("risk"));
		assertEquals("Alarm", m.get("handling"));
		assertEquals("", m.get("info"));
		assertEquals(137, m.get("src_port"));

	}

	@Test
	public void testPocSample2() {
		String log = "<36>[SNIPER-2000] [Attack_Name=(0023)UDP Check Sum Error], [Time=2013/05/14 14:32:05], "
				+ "[Hacker=130.1.254.133], [Victim=130.1.213.10], [Protocol=udp/514], [Risk=Medium], "
				+ "[Handling=Alarm], [Information=], [SrcPort=514]";
		SniperLogParser p = new SniperLogParser();
		Map<String, Object> m = p.parse(line(log));

		assertEquals("(0023)UDP Check Sum Error", m.get("attack_name"));
		assertEquals(date(2013, 5, 14, 14, 32, 5), m.get("_time"));
		assertEquals("130.1.254.133", m.get("hacker"));
		assertEquals("130.1.213.10", m.get("victim"));
		assertEquals("udp/514", m.get("protocol"));
		assertEquals("Medium", m.get("risk"));
		assertEquals("Alarm", m.get("handling"));
		assertEquals("", m.get("info"));
		assertEquals(514, m.get("src_port"));
	}

	@Test
	public void testPocSample3() {
		String log = "<36>[SNIPER-2000] [Attack_Name=(0183)SNMP Sweep (community string)], "
				+ "[Time=2013/05/14 14:28:03], [Hacker=130.1.1.230], [Victim=130.1.168.231], "
				+ "[Protocol=udp/161], [Risk=High], [Handling=Alarm], [Information=], [SrcPort=44732]";
		SniperLogParser p = new SniperLogParser();
		Map<String, Object> m = p.parse(line(log));

		assertEquals("(0183)SNMP Sweep (community string)", m.get("attack_name"));
		assertEquals(date(2013, 5, 14, 14, 28, 3), m.get("_time"));
		assertEquals("130.1.1.230", m.get("hacker"));
		assertEquals("130.1.168.231", m.get("victim"));
		assertEquals("udp/161", m.get("protocol"));
		assertEquals("High", m.get("risk"));
		assertEquals("Alarm", m.get("handling"));
		assertEquals("", m.get("info"));
		assertEquals(44732, m.get("src_port"));
	}

	@Test
	public void testPocSample4() {
		String log = "<36>[SNIPER-2000] [Attack_Name=(0240)ARP Reply Poison(havoc)], "
				+ "[Time=2013/05/14 14:32:14], [Hacker=0.0.0.0], [Victim=0.0.0.0], [Protocol=etc/0], "
				+ "[Risk=High], [Handling=Alarm], [Information=], [SrcPort=0]";
		SniperLogParser p = new SniperLogParser();
		Map<String, Object> m = p.parse(line(log));

		assertEquals("(0240)ARP Reply Poison(havoc)", m.get("attack_name"));
		assertEquals(date(2013, 5, 14, 14, 32, 14), m.get("_time"));
		assertEquals("0.0.0.0", m.get("hacker"));
		assertEquals("0.0.0.0", m.get("victim"));
		assertEquals("etc/0", m.get("protocol"));
		assertEquals("High", m.get("risk"));
		assertEquals("Alarm", m.get("handling"));
		assertEquals("", m.get("info"));
		assertEquals(0, m.get("src_port"));
	}

	@Test
	public void testPocSample5() {
		String log = "<36>[SNIPER-2000] [Attack_Name=(0400)SMB Service connect(tcp-445)], "
				+ "[Time=2013/05/14 14:32:06], [Hacker=140.1.184.80], [Victim=130.1.2.17], "
				+ "[Protocol=tcp/445], [Risk=Medium], [Handling=Alarm], [Information=], [SrcPort=3238]";
		SniperLogParser p = new SniperLogParser();
		Map<String, Object> m = p.parse(line(log));

		assertEquals("(0400)SMB Service connect(tcp-445)", m.get("attack_name"));
		assertEquals(date(2013, 5, 14, 14, 32, 6), m.get("_time"));
		assertEquals("140.1.184.80", m.get("hacker"));
		assertEquals("130.1.2.17", m.get("victim"));
		assertEquals("tcp/445", m.get("protocol"));
		assertEquals("Medium", m.get("risk"));
		assertEquals("Alarm", m.get("handling"));
		assertEquals("", m.get("info"));
		assertEquals(3238, m.get("src_port"));
	}

	@Test
	public void testPocSample6() {
		String log = "<36>[SNIPER-2000] [Attack_Name=(0062)FTP Login Fail], [Time=2013/05/14 14:32:25], "
				+ "[Hacker=130.1.9.61], [Victim=130.1.1.133], [Protocol=tcp/21], [Risk=Medium], "
				+ "[Handling=Alarm], [Information=userid [], passwd []], [SrcPort=64257]";
		SniperLogParser p = new SniperLogParser();
		Map<String, Object> m = p.parse(line(log));

		assertEquals("(0062)FTP Login Fail", m.get("attack_name"));
		assertEquals(date(2013, 5, 14, 14, 32, 25), m.get("_time"));
		assertEquals("130.1.9.61", m.get("hacker"));
		assertEquals("130.1.1.133", m.get("victim"));
		assertEquals("tcp/21", m.get("protocol"));
		assertEquals("Medium", m.get("risk"));
		assertEquals("Alarm", m.get("handling"));
		assertEquals("userid [], passwd []", m.get("info"));
		assertEquals(64257, m.get("src_port"));
	}

	@Test
	public void testPocSample7() {
		String log = "<36>[SNIPER-2000] [Attack_Name=(0601)DHCP Discover Flooding], [Time=2013/05/14 14:29:04], "
				+ "[Hacker=0.0.0.0], [Victim=255.255.255.255], [Protocol=udp/67], [Risk=Medium], "
				+ "[Handling=Alarm], [Information=CMAC=00032E0803DB,], [SrcPort=68]";
		SniperLogParser p = new SniperLogParser();
		Map<String, Object> m = p.parse(line(log));

		assertEquals("(0601)DHCP Discover Flooding", m.get("attack_name"));
		assertEquals(date(2013, 5, 14, 14, 29, 4), m.get("_time"));
		assertEquals("0.0.0.0", m.get("hacker"));
		assertEquals("255.255.255.255", m.get("victim"));
		assertEquals("udp/67", m.get("protocol"));
		assertEquals("Medium", m.get("risk"));
		assertEquals("Alarm", m.get("handling"));
		assertEquals("CMAC=00032E0803DB,", m.get("info"));
		assertEquals(68, m.get("src_port"));
	}

	@Test
	public void testPocSample8() {
		String log = "<36>[SNIPER-2000] [Attack_Name=(0003)FIN Port Scan], [Time=2013/05/14 14:31:59], "
				+ "[Hacker=172.21.9.28], [Victim=130.1.1.84], [Protocol=tcp/63882], [Risk=Medium], "
				+ "[Handling=Alarm], [Information=], [SrcPort=13724]";
		SniperLogParser p = new SniperLogParser();
		Map<String, Object> m = p.parse(line(log));

		assertEquals("(0003)FIN Port Scan", m.get("attack_name"));
		assertEquals(date(2013, 5, 14, 14, 31, 59), m.get("_time"));
		assertEquals("172.21.9.28", m.get("hacker"));
		assertEquals("130.1.1.84", m.get("victim"));
		assertEquals("tcp/63882", m.get("protocol"));
		assertEquals("Medium", m.get("risk"));
		assertEquals("Alarm", m.get("handling"));
		assertEquals("", m.get("info"));
		assertEquals(13724, m.get("src_port"));
	}

	@Test
	public void testPocSample9() {
		String log = "<36>[SNIPER-2000] [Attack_Name=(0006)UDP Flooding], [Time=2013/05/14 14:32:24], "
				+ "[Hacker=130.1.254.6], [Victim=130.1.2.142], [Protocol=udp/2376], [Risk=High], "
				+ "[Handling=Alarm], [Information=], [SrcPort=161]";
		SniperLogParser p = new SniperLogParser();
		Map<String, Object> m = p.parse(line(log));

		assertEquals("(0006)UDP Flooding", m.get("attack_name"));
		assertEquals(date(2013, 5, 14, 14, 32, 24), m.get("_time"));
		assertEquals("130.1.254.6", m.get("hacker"));
		assertEquals("130.1.2.142", m.get("victim"));
		assertEquals("udp/2376", m.get("protocol"));
		assertEquals("High", m.get("risk"));
		assertEquals("Alarm", m.get("handling"));
		assertEquals("", m.get("info"));
		assertEquals(161, m.get("src_port"));
	}

	@Test
	public void testPocSample10() {
		String log = "<36>[SNIPER-2000] [Attack_Name=(5450)index.jsp (JSP Request Source Code Disclosure Vulnerability)], "
				+ "[Time=2013/05/14 14:22:10], [Hacker=130.1.211.3], [Victim=130.1.2.86], [Protocol=tcp/80], "
				+ "[Risk=Low], [Handling=Alarm], [Information=GET /sim/index.jsp HTTP/1.1], [SrcPort=37793]";
		SniperLogParser p = new SniperLogParser();
		Map<String, Object> m = p.parse(line(log));

		assertEquals("(5450)index.jsp (JSP Request Source Code Disclosure Vulnerability)", m.get("attack_name"));
		assertEquals(date(2013, 5, 14, 14, 22, 10), m.get("_time"));
		assertEquals("130.1.211.3", m.get("hacker"));
		assertEquals("130.1.2.86", m.get("victim"));
		assertEquals("tcp/80", m.get("protocol"));
		assertEquals("Low", m.get("risk"));
		assertEquals("Alarm", m.get("handling"));
		assertEquals("GET /sim/index.jsp HTTP/1.1", m.get("info"));
		assertEquals(37793, m.get("src_port"));
	}

	@Test
	public void testPocSample11() {
		String log = "<36>[SNIPER-2000] [Attack_Name=(5028)/cgi-bin/ HTTP (cgi-bin Directory view)], "
				+ "[Time=2013/05/14 14:24:50], [Hacker=130.1.33.77], [Victim=130.1.2.25], "
				+ "[Protocol=tcp/8080], [Risk=Low], [Handling=Alarm], "
				+ "[Information=OPTIONS /cognos10/cgi-bin/ HTTP/1.1], [SrcPort=1311]";
		SniperLogParser p = new SniperLogParser();
		Map<String, Object> m = p.parse(line(log));

		assertEquals("(5028)/cgi-bin/ HTTP (cgi-bin Directory view)", m.get("attack_name"));
		assertEquals(date(2013, 5, 14, 14, 24, 50), m.get("_time"));
		assertEquals("130.1.33.77", m.get("hacker"));
		assertEquals("130.1.2.25", m.get("victim"));
		assertEquals("tcp/8080", m.get("protocol"));
		assertEquals("Low", m.get("risk"));
		assertEquals("Alarm", m.get("handling"));
		assertEquals("OPTIONS /cognos10/cgi-bin/ HTTP/1.1", m.get("info"));
		assertEquals(1311, m.get("src_port"));
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
