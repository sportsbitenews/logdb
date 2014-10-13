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

public class GenianNacSnmpLogParserTest {

	@Test
	public void testGeni() {
		String line = "2014-08-27 14:11:52 INFO 900 NONE 0.0.0.0 NONE 세션 타임아웃에 의해 관리자가 로그아웃 됨. ADMIN=admin, ADMIN_IP=172.16.116.53 NONE";
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("1.3.6.1.4.1.29503.1.1.0.100", line);
		m.put("generic_trap", "coldStart");
		m.put("_table", "SNP_NAC_172_31_7_250");
		m.put("_time", "2014-08-27 14:14:06+0900");
		m.put("_host", "172.31.7.250");

		GenianNacSnmpLogParser parser = new GenianNacSnmpLogParser();
		Map<String, Object> parsed = parser.parse(m);

		assertEquals("2014-08-27 14:11:52", parsed.get("datetime"));
		assertEquals("INFO", parsed.get("logtype"));
		assertEquals("900", parsed.get("logid"));
		assertEquals("NONE", parsed.get("sensorname"));
		assertEquals("0.0.0.0", parsed.get("ip"));
		assertEquals("NONE", parsed.get("mac"));
		assertEquals("세션 타임아웃에 의해 관리자가 로그아웃 됨.", parsed.get("fullmsg"));
		assertEquals("ADMIN=admin, ADMIN_IP=172.16.116.53 NONE", parsed.get("detailmsg"));
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
