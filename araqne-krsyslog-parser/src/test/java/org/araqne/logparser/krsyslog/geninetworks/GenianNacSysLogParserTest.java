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
package org.araqne.logparser.krsyslog.geninetworks;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Ignore;
import org.junit.Test;

/**
 * @author kyun
 */

public class GenianNacSysLogParserTest {

	@Test
	public void testGeni() {
		String line = "2014-09-30 20:14:50 INFO 108 218.49.19.17 218.49.21.237 30:0E:D5:16:0A:82 패치설치 실패함. "
				+ "NAME=Update for Microsoft XML Core Services 4.0 Service Pack 2 for x64-based Systems (KB973688), ID=9BD35FB2-8618-4F00-B75D-DFD8D7E93278  MSG=Result = 0x80070643 - ERROR_INSTALL_FAILURE -  NONE";
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);

		GenianNacSysLogParser parser = new GenianNacSysLogParser();
		Map<String, Object> parsed = parser.parse(m);

		assertEquals("2014-09-30 20:14:50", parsed.get("datetime"));
		assertEquals("INFO", parsed.get("logtype"));
		assertEquals("108", parsed.get("logid"));
		assertEquals("218.49.19.17", parsed.get("sensorname"));
		assertEquals("218.49.21.237", parsed.get("ip"));
		assertEquals("30:0E:D5:16:0A:82", parsed.get("mac"));
		assertEquals("패치설치 실패함.", parsed.get("fullmsg"));
		assertEquals("NAME=Update for Microsoft XML Core Services 4.0 Service Pack 2 for x64-based Systems (KB973688), ID=9BD35FB2-8618-4F00-B75D-DFD8D7E93278  MSG=Result = 0x80070643 - ERROR_INSTALL_FAILURE -  NONE", 
				parsed.get("detailmsg"));
	}

	@Ignore
	@Test
	public void test2() {
		long s = System.currentTimeMillis();
		final long CNT = 5000000;
		for (int i = 0; i < CNT; i++)
			testGeni();
		long e = System.currentTimeMillis() - s;

		System.out.println(e + " " + (CNT * 1000 / e));

	}
}
