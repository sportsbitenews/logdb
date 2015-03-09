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

import org.araqne.logparser.krsyslog.secui.Mf2LogParser;
import org.araqne.logparser.krsyslog.secui.Mf2LogParser.Mode;
import org.junit.Test;

/**
 * @since 1.9.2
 * @author mindori
 * 
 *         rule of function signature: [(CSV|TSV|WELF)]Test[log number(e.g. 1_4
 *         means 1.4 HA Status Log)] see also: TEC-08-11_001_SECUI MF2 Syslog
 *         전송포맷 V2.0.pdf
 * 
 */
public class Mf2LogParserTest {
	@Test
	public void testBrokenLogParse() {
		String line = "<190>1 2015-02-17T04:35:41.384736Z [ips_ddos_incident] [210.223.182.1] 2015-02-17 13:35:37,2015-02-17 13:35:37,DaeYang,#14715(HTTP Authorization Login Brute Force Attempt-1/3(count 10, seconds 1)),Low,#0(IPS_DDOS占쏙옙占?,1,1518,detect,10425309775129";

		Mf2LogParser p = new Mf2LogParser(Mode.CSV);	
		Map<String, Object> m = p.parse(line(line));
		System.out.println(m);

	}
	
	@Test
	public void testBrokenLogParse2() {
		String line = "<190>1 2015-02-16T09:17:33.113629Z [vpn_act_ike] [210.223.182.1] 2015-02-16 18:17:33,DaeYang,-,0.0.0.0,-,SUCCESS,DEBUG,[exchange_free_aux:L2372] exchange (ptr=0x1c27c80, name=nil, remote_addr=0.0.0.0, phase=1) released.";

		Mf2LogParser p = new Mf2LogParser(Mode.CSV);	
		Map<String, Object> m = p.parse(line(line));
		System.out.println(m);

	}
	
	@Test
	public void csvTest() {
		Mf2LogParser p = new Mf2LogParser(Mode.CSV);
		Map<String, Object> m = p.parse(line("<190>1 2015-03-09T05:57:56.796286Z [fw4_deny] [222.119.190.2] 2015-03-09 14:57:56,2015-03-09 14:57:56,0,DaeYang2,5,Undefined,184.105.139.67,32873,222.119.190.229,161,UDP,EXT,1,131, ,-,Deny by Deny Rule"));
		assertEquals("fw4_deny", m.get("log_type"));
		assertEquals("222.119.190.2", m.get("from_ip"));
		assertEquals("2015-03-09 14:57:56", m.get("stime"));
		assertEquals("DaeYang2", m.get("machid"));
		assertEquals(" ", m.get("fragment"));
	}

	@Test
	public void tsvTest() {
		Mf2LogParser p = new Mf2LogParser(Mode.TSV);
		Map<String, Object> m = p.parse(line(convertTsv("<190>1 2015-03-09T05:57:56.781774Z [ips_ddos_domain_traffic_proto] [222.119.190.2] 2015-03-09 14:57:56,DaeYang2,#0(defaultDY),589.0,0.0,4859.5,2.6,2.0,0.0,0.0,0.0,509.2,0.8,3369.5,4.6,0.7,0.0,0.0,0.0")));
		assertEquals("ips_ddos_domain_traffic_proto", m.get("log_type"));
		assertEquals("222.119.190.2", m.get("from_ip"));
		assertEquals("2015-03-09 14:57:56", m.get("time"));
		assertEquals("589.0", m.get("inbound_tcp"));
	}

	private Map<String, Object> line(String line) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);
		return m;
	}

	private String convertTsv(String line) {
		return line.replaceAll(",", "\t");
	}
}
