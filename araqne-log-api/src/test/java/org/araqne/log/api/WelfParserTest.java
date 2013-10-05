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
package org.araqne.log.api;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import static org.junit.Assert.*;

public class WelfParserTest {
	@Test
	public void testSample() {
		// from https://www.trustwave.com/support/kb/article.aspx?id=10899
		String line = "id=firewall time=\"2000-2-4 12:01:01\" fw=192.168.0.238 pri=6 rule=3 " +
				"proto=http src=192.168.0.23 dst 6.1.0.36 rg=www.webtrends.com/index.html op=GET result 0 rcvd=1426";
		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		WelfParser p = new WelfParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("firewall", m.get("id"));
		assertEquals("2000-2-4 12:01:01", m.get("time"));
		assertEquals("192.168.0.238", m.get("fw"));
		assertEquals("6", m.get("pri"));
		assertEquals("3", m.get("rule"));
		assertEquals("http", m.get("proto"));
		assertEquals("192.168.0.23", m.get("src"));
		assertEquals("6.1.0.36", m.get("dst"));
		assertEquals("www.webtrends.com/index.html", m.get("rg"));
		assertEquals("GET", m.get("op"));
		assertEquals("0", m.get("result"));
		assertEquals("1426", m.get("rcvd"));
	}

	@Test
	public void testJuniperWelfLog() {
		String line = "id=firewall time=\"2012-04-17 12:00:18\" pri=6 fw=10.36.1.2 vpn=ive "
				+ "user=DOMAIN\\user1 realm=\"domain.local\" roles=\"Basic\" proto=http src=192.168.204.11 "
				+ "dst=10.35.0.76 dstname=myserver.domain.local type=vpn op=GET "
				+ "arg=\"/RH/Content.aspx?LN12Sds/yLgv/zM2lEOHXLC7qfg7FKTKKP3SvJM/UgCVp1sMT3gXkmiztVSAmgz2\" "
				+ "result=200 sent=96 rcvd=1156 agent=\"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)\" duration=0 msg=\"WEB20174: WebRequest completed, GET to http://myserver.domain.local:80//RH/Contenido.aspx?LN12Sds/yLgv/zM2lEOHXLC7qfg7FKTKKP3SvJM/UgCVp1sMT3gXkmiztVSAmgz2 from 10.35.0.76 result=200 sent=96 received=1156 in 0 seconds\"";
		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		WelfParser p = new WelfParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("firewall", m.get("id"));
		assertEquals("2012-04-17 12:00:18", m.get("time"));
		assertEquals("6", m.get("pri"));
		assertEquals("10.36.1.2", m.get("fw"));
		assertEquals("ive", m.get("vpn"));
		assertEquals("DOMAIN\\user1", m.get("user"));
		assertEquals("domain.local", m.get("realm"));
		assertEquals("Basic", m.get("roles"));
		assertEquals("http", m.get("proto"));
		assertEquals("192.168.204.11", m.get("src"));
		assertEquals("10.35.0.76", m.get("dst"));
		assertEquals("myserver.domain.local", m.get("dstname"));
		assertEquals("vpn", m.get("type"));
		assertEquals("GET", m.get("op"));
		assertEquals("/RH/Content.aspx?LN12Sds/yLgv/zM2lEOHXLC7qfg7FKTKKP3SvJM/UgCVp1sMT3gXkmiztVSAmgz2", m.get("arg"));
		assertEquals("200", m.get("result"));
		assertEquals("96", m.get("sent"));
		assertEquals("1156", m.get("rcvd"));
		assertEquals(
				"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)",
				m.get("agent"));
	}

	@Test
	public void parseTessPacketLog() {
		String line = "EventName=\"http sql injection keyword %252e\" SigIndex=8110 Severity=Middle " +
				"Time=\"2013/09/24 13:30:58\" Protocol=TCP AttackerIP=192.168.13.10 AttackerPort=3887 " +
				"VictimIP=1.2.3.4 VictimPort=80 Count=1 PktCount=1 Pattern=\"%252E\" " +
				"Direct= SensorIP=192.168.10.1 Sensor=demo Network=NODATA " +
				"VSensor=demo Packet=\"00 09 0F 09 00 07 00 1B ED AD ";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		WelfParser p = new WelfParser();
		Map<String, Object> m = p.parse(log);
		System.out.println(m);

		assertEquals("http sql injection keyword %252e", m.get("EventName"));
		assertEquals("8110", m.get("SigIndex"));
		assertEquals("Middle", m.get("Severity"));
		assertEquals("1.2.3.4", m.get("VictimIP"));
		assertEquals("80", m.get("VictimPort"));
		assertEquals("1", m.get("Count"));
		assertEquals("1", m.get("PktCount"));
		assertEquals("%252E", m.get("Pattern"));
		assertEquals("", m.get("Direct"));
		assertEquals("192.168.10.1", m.get("SensorIP"));
		assertEquals("demo", m.get("Sensor"));
		assertEquals("NODATA", m.get("Network"));
		assertEquals("demo", m.get("VSensor"));

		// if value has no closing quote, last char will be truncated
		assertEquals("00 09 0F 09 00 07 00 1B ED A", m.get("Packet"));
	}
}
