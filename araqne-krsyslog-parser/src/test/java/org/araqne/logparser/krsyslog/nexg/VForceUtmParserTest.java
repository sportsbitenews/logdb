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
package org.araqne.logparser.krsyslog.nexg;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.araqne.logparser.krsyslog.nexg.VForceUtmParser;
import org.junit.Test;

public class VForceUtmParserTest {
	@Test
	public void testSample() {
		String line = "Apr 14 03:19:34 session: NZC25081310046 Proto:1, Policy:pass, Rule:1, Type:open, Src:10.10.81.16, Dst:10.10.81.1, Spt_c:0, Dpt_t:8, Bytes:0, Packets:0, Repl_Src:10.10.81.1, Repl_Dst:10.10.81.16, Repl_Spt_c:0, Repl_Dpt_t:0, Repl_Bytes:0, Repl_Packets:0, Count:1, Start_Time:Apr 14 03:19:34, End_Time:-";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		VForceUtmParser p = new VForceUtmParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("Apr 14 03:19:34", m.get("datetime"));
		assertEquals("session", m.get("log_category"));
		assertEquals("1", m.get("proto"));
		assertEquals("10.10.81.16", m.get("src"));
		assertEquals("Apr 14 03:19:34", m.get("start_time"));
		assertEquals("-", m.get("end_time"));
	}

	@Test
	public void testSample2() {
		String line = "Apr 14 17:42:53 appgw_http: NZC25081310046 Src:10.10.81.49, Dst:23.76.153.59, Action:pass, Name:http-filter-test-1, Rule:9999, Host:download.cdn.mozilla.net, Path:/pub/firefox/releases/37.0.1/update/win32/ko/firefox-37.0.1.complete.mar";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		VForceUtmParser p = new VForceUtmParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("Apr 14 17:42:53", m.get("datetime"));
		assertEquals("appgw_http", m.get("log_category"));
		assertEquals("http-filter-test-1", m.get("name"));
		assertEquals("/pub/firefox/releases/37.0.1/update/win32/ko/firefox-37.0.1.complete.mar", m.get("path"));
	}

	@Test
	public void testSample3() {
		String line = "Jul 7 10:12:12 session: NZC25081310046 Proto:6, Policy:pass, Rule:90, Type:close-RST, Src:201.232.182.39, Dst:112.156.130.230, Spt_c:2085, Dpt_t:17880, Bytes:1109, Packets:6, Repl_Src:112.136.170.230, Repl_Dst:211.232.102.39, Repl_Spt_c:17880, Repl_Dpt_t:2085, Repl_Bytes:2328, Repl_Packets:5, Count:1, Start_Time:Jul 7 10:11:44, End_Time:Jul 7 10:12:12";

		HashMap<String, Object> log = new HashMap<String, Object>();
		log.put("line", line);

		VForceUtmParser p = new VForceUtmParser();
		Map<String, Object> m = p.parse(log);

		assertEquals("6", m.get("proto"));
		assertEquals("close-RST", m.get("type"));
	}
}
