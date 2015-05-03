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
package org.araqne.logparser.krsyslog.monitorapp;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class WebInsightParserTest {

	@Test
	public void testDetectLog() {
		String line = "DETECT|10.0.1.206|v3|2014-02-04 17:37:51|1|SQL_INJECTION|1:SQL Injection(1)|10.0.1.94|52923|10.0.1.205|80|query/payload “’ or 1=1?“|BLOCK|MEDIUM|http|www.qctest.co.kr |291|GET /bbs/zbonard.php?id=test%20'%20or%201=1-- HTTP/1.1 Accept: text/html, application/xhtml+xml, */* Accept-Language: ko-KR User-Agent: Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko Accept-Encoding: gzip, deflate Host: www.qctest.co.kr Connection: Keep-Alive";

		WebInsightParser parser = new WebInsightParser();
		Map<String, Object> args = new HashMap<String, Object>();
		args.put("line", line);
		Map<String, Object> m = parser.parse(args);

		assertEquals("DETECT", m.get("log_type"));
		assertEquals("10.0.1.206", m.get("mgmt_ip"));
		assertEquals("v3", m.get("version"));
		assertEquals("2014-02-04 17:37:51", m.get("time"));
		assertEquals("1", m.get("detect_code_num"));
		assertEquals("SQL_INJECTION", m.get("detect_type"));
		assertEquals("1:SQL Injection(1)", m.get("rule_name"));
		assertEquals("10.0.1.94", m.get("client_ip"));
		assertEquals("52923", m.get("client_port"));
		assertEquals("10.0.1.205", m.get("server_ip"));
		assertEquals("80", m.get("server_port"));
		assertEquals("query/payload “’ or 1=1?“", m.get("detect_contents"));
		assertEquals("BLOCK", m.get("action"));
		assertEquals("MEDIUM", m.get("severity"));
		assertEquals("http", m.get("protocol"));
		assertEquals("www.qctest.co.kr", m.get("host"));
		assertEquals("291", m.get("request_len"));
		assertEquals("GET /bbs/zbonard.php?id=test%20'%20or%201=1-- HTTP/1.1 Accept: text/html, application/xhtml+xml,"
				+ " */* Accept-Language: ko-KR User-Agent: Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like "
				+ "Gecko Accept-Encoding: gzip, deflate Host: www.qctest.co.kr Connection: Keep-Alive", m.get("request_data"));
	}

	@Test
	public void testSystemLog() {
		String line = "SYSTEM|10.0.1.206|v3|2014-02-04 17:34:50|1%|20%|1%|(eth1 DOWN,0,unknown)(eth2 DOWN,0,unknown)(mgmt UP, 100,full)|0|0cps|0tps|0bps|6";

		WebInsightParser parser = new WebInsightParser();
		Map<String, Object> args = new HashMap<String, Object>();
		args.put("line", line);
		Map<String, Object> m = parser.parse(args);

		assertEquals("SYSTEM", m.get("log_type"));
		assertEquals("10.0.1.206", m.get("mgmt_ip"));
		assertEquals("v3", m.get("version"));
		assertEquals("2014-02-04 17:34:50", m.get("time"));
		assertEquals("1%", m.get("cpu_avg"));
		assertEquals("20%", m.get("mem_avg"));
		assertEquals("1%", m.get("disk_avg"));
		assertEquals("(eth1 DOWN,0,unknown)(eth2 DOWN,0,unknown)(mgmt UP, 100,full)", m.get("link_status"));
		assertEquals("0", m.get("open_connection"));
		assertEquals("0cps", m.get("cps"));
		assertEquals("0tps", m.get("tps"));
		assertEquals("0bps", m.get("bps"));
		assertEquals("6", m.get("httpgw_status"));

	}

}