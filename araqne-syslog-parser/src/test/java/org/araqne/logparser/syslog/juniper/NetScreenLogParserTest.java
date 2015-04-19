/*
 * Copyright 2012 Future Systems
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

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class NetScreenLogParserTest {
	@Test
	public void testSample() {
		NetScreenLogParser parser = new NetScreenLogParser();
		Map<String, Object> m = parser
				.parse(line("JW_FW_M: NetScreen device_id=JW_FW_M  [Root]system-alert-00012: UDP flood! From 172.24.94.229:36026 to 118.131.89.60:15937, proto UDP (zone V1-Trust int  v1-trust). Occurred 2 times. (2015-04-15 15:23:18)"));
		assertEquals(" UDP flood! From 172.24.94.229:36026 to 118.131.89.60:15937, proto UDP (zone V1-Trust int  v1-trust). Occurred 2 times. (2015-04-15 15:23:18)", m.get("message"));
	}

	private Map<String, Object> line(String line) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);
		return m;
	}
}
