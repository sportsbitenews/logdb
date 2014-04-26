/*
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
package org.araqne.logparser.syslog.penta;


import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * @author kyun
 */
public class WapplesSyslogParserTest {

	 @Test
	public void testParser() {
		String line = "Mar 18 19:12:44 SKMC-WAF-OCB syslogmd: INTRUSION DETECTION TIME : 14/3/18 19:12:44 SOURCE IP : 125.141.71.81 "
				+ "URI : /event/service/appAuthTocb/appAuthTocbMain.mocb|65806 RULE NAME : ExtensionFiltering "
				+ "RAW DATA : GET /event/service/appAuthTocb/appAuthTocbMain.mocb%7C65806?app_type=ios&mbr_id=g5SNUcBnDh2MxqeLZHI33g%3D%3D&CI_YN=Y HTTP/1.1 Host: m.okcashbag.com Accept-Encoding: gzip, deflate Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8 Cookie: cookie_id=201403101733369775393 Connection: keep-alive Accept-Language: ja-jp User-Agent: Mozilla/5.0 (iPhone; CPU iPhone OS 7_1 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Mobile/11D167 HOST NAME : m.okcashbag.com DESTINATION IP : 203.235.200.42:80 "
				+ "RESPONSE TYPE : Error code RISK : 0";

		WapplesSyslogParser parser = new WapplesSyslogParser();
		Map<String, Object> m = parser.parse(line(line));

		assertEquals("14/3/18 19:12:44", m.get("intrusion_detection_time"));
		assertEquals("125.141.71.81", m.get("source_ip"));
		assertEquals("/event/service/appAuthTocb/appAuthTocbMain.mocb|65806", m.get("uri"));
		assertEquals("ExtensionFiltering", m.get("rule_name"));
		assertEquals(
				"GET /event/service/appAuthTocb/appAuthTocbMain.mocb%7C65806?"
				+ "app_type=ios&mbr_id=g5SNUcBnDh2MxqeLZHI33g%3D%3D&CI_YN=Y HTTP/1.1 Host: m"
				+ ".okcashbag.com Accept-Encoding: gzip, deflate Accept: text/html,applicati"
				+ "on/xhtml+xml,application/xml;q=0.9,*/*;q=0.8 Cookie: cookie_id=2014031017"
				+ "33369775393 Connection: keep-alive Accept-Language: ja-jp User-Agent: Moz"
				+ "illa/5.0 (iPhone; CPU iPhone OS 7_1 like Mac OS X) AppleWebKit/537.51.2 ("
				+ "KHTML, like Gecko) Mobile/11D167",
				m.get("raw_data"));
		assertEquals("m.okcashbag.com", m.get("host_name"));
		assertEquals("203.235.200.42:80", m.get("destination_ip"));
		assertEquals("Error code", m.get("response_type"));
		assertEquals("0", m.get("risk"));
	}
	
	@Test
	public void testParser2() {
		String line = "Apr  9 15:01:09 SKMC-WAF-NOS syslogmd:  NETWORK  CPS : 15  TPS : 71  TRANSACTION SIZE(Kbyte) : 654  BYPASS : OFF";

		WapplesSyslogParser parser = new WapplesSyslogParser();
		Map<String, Object> m = parser.parse(line(line));

		assertEquals("15", m.get("network__cps"));
		assertEquals("71", m.get("tps"));
		assertEquals("654", m.get("transaction_size(kbyte)"));
		assertEquals("OFF", m.get("bypass"));
		
	}
	
	@Test
	public void testParser3() {
		String line = "Apr  9 15:01:09 SKMC-WAF-NOS syslogmd:  SYSTEM  CPU USED : 3.3851 % MEM USED : 29.4535%";
		WapplesSyslogParser parser = new WapplesSyslogParser();
		Map<String, Object> m = parser.parse(line(line));
		assertEquals("3.3851 %", m.get("system__cpu_used"));
		assertEquals("29.4535%", m.get("mem_used"));
	}
	
	private Map<String, Object> line(String line) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);
		return m;
	}
}
