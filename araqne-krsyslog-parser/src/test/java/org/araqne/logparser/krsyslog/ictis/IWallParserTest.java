/**
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
package org.araqne.logparser.krsyslog.ictis;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.araqne.logparser.krsyslog.ictis.IWallParser;
import org.junit.Test;

public class IWallParserTest {

	@Test
	public void testAuditLog() {
		String line = "2015-04-13 11:23:56 logtest.ictis.kr UI: prefix=AAL type=audit "
				+ "msg=\"2015-04-13 11:23:56;admin;10.0.0.2;3;SM;System Setting;Update System Information - Host Name : logtest / Domain : ictis.com;Success\"";

		IWallParser parser = new IWallParser();
		Map<String, Object> args = new HashMap<String, Object>();
		args.put("line", line);
		Map<String, Object> m = parser.parse(args);

		assertEquals("2015-04-13 11:23:56", m.get("time"));
		assertEquals("logtest.ictis.kr", m.get("machine_name"));
		assertEquals("UI", m.get("system_name"));
		assertEquals("AAL", m.get("prefix"));
		assertEquals("audit", m.get("type"));
		assertEquals(
				"2015-04-13 11:23:56;admin;10.0.0.2;3;SM;System Setting;Update System Information - Host Name : logtest / Domain : ictis.com;Success",
				m.get("msg"));
		assertEquals("2015-04-13 11:23:56", m.get("log_time"));
		assertEquals("admin", m.get("user"));
		assertEquals("10.0.0.2", m.get("user_ip"));
		assertEquals("3", m.get("authority"));
		assertEquals("SM", m.get("main_category"));
		assertEquals("System Setting", m.get("sub_category"));
		assertEquals("Update System Information - Host Name : logtest / Domain : ictis.com", m.get("action"));
		assertEquals("Success", m.get("result"));
	}

	@Test
	public void testDefenceLog() {
		String line = "2015-04-13 11:44:37 logtest.ictis.com LogDaemon: prefix=LDA type=defence "
				+ "ruleid=1 in=eth4 out=eth5 srcip=192.168.0.99 spt=9401 dstip=192.168.0.159 dpt=56707 protocol=UDP len=51";

		IWallParser parser = new IWallParser();
		Map<String, Object> args = new HashMap<String, Object>();
		args.put("line", line);
		Map<String, Object> m = parser.parse(args);

		assertEquals("2015-04-13 11:44:37", m.get("time"));
		assertEquals("logtest.ictis.com", m.get("machine_name"));
		assertEquals("LogDaemon", m.get("system_name"));
		assertEquals("LDA", m.get("prefix"));
		assertEquals("defence", m.get("type"));
		assertEquals("1", m.get("ruleid"));
		assertEquals("eth4", m.get("in"));
		assertEquals("eth5", m.get("out"));
		assertEquals("192.168.0.99", m.get("srcip"));
		assertEquals("9401", m.get("spt"));
		assertEquals("192.168.0.159", m.get("dstip"));
		assertEquals("56707", m.get("dpt"));
		assertEquals("UDP", m.get("protocol"));
		assertEquals("51", m.get("len"));
	}

	@Test
	public void testPacketLog() {
		String line = "2015-04-13 11:50:20 logtest.ictis.com LogDaemon: prefix=LAM type=deny ruleid=65535 in=eth4 out=eth5 "
				+ "srcip=192.168.0.130 spt=137 dstip=192.168.0.255 dpt=137 protocol=UDP len=78";

		IWallParser parser = new IWallParser();
		Map<String, Object> args = new HashMap<String, Object>();
		args.put("line", line);
		Map<String, Object> m = parser.parse(args);

		assertEquals("2015-04-13 11:50:20", m.get("time"));
		assertEquals("logtest.ictis.com", m.get("machine_name"));
		assertEquals("LogDaemon", m.get("system_name"));
		assertEquals("LAM", m.get("prefix"));
		assertEquals("deny", m.get("type"));
		assertEquals("65535", m.get("ruleid"));
		assertEquals("eth4", m.get("in"));
		assertEquals("eth5", m.get("out"));
		assertEquals("192.168.0.130", m.get("srcip"));
		assertEquals("137", m.get("spt"));
		assertEquals("192.168.0.255", m.get("dstip"));
		assertEquals("137", m.get("dpt"));
		assertEquals("UDP", m.get("protocol"));
		assertEquals("78", m.get("len"));
	}

}
