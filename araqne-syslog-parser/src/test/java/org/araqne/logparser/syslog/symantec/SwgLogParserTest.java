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
package org.araqne.logparser.syslog.symantec;

import static org.junit.Assert.assertEquals;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * @author kyun
 */
public class SwgLogParserTest {
		
	@Test
	public void testParser() {
		String line = "04/11/2014 15:00,10.202.211.138,10.202.211.138,,Advertising,Ordering,Minor,Monitored,Content Filter,173.194.126.249,80,2"
				+ ",googlesyndication.com,\"pagead2.googlesyndication.com/activeview?id=osdim&avi=BHL982oRHU6n7KoKu8AWH1oDQAgDE5Nj4jgEAABABOAHIAQKgBgKoE4AB&adk=164142136&p=33,494,93,962&tos=0,0,0,0,0&mtos=0,0,0,0,0&rs=1&ht=0&fp=client%3Dca-pub-5967860900629716%26url%3Dhttp%253A%252F%252Fwww.82cook\"";
	
		SwgLogParser parser = new SwgLogParser();
		Map<String, Object> m = parser.parse(line(line));
		
		Date time = (Date) m.get("timestamp");
		Calendar c = Calendar.getInstance();
		c.setTime(time);
		assertEquals(2014, c.get(Calendar.YEAR));
		assertEquals(3, c.get(Calendar.MONTH));
		assertEquals(11, c.get(Calendar.DAY_OF_MONTH));
		assertEquals(15, c.get(Calendar.HOUR_OF_DAY));
		assertEquals(00, c.get(Calendar.MINUTE));
		
		assertEquals("10.202.211.138", m.get("hostname"));
		assertEquals("10.202.211.138", m.get("local_ip"));
		assertEquals(null, m.get("detection"));
		assertEquals("Advertising", m.get("category"));
		assertEquals("Ordering", m.get("class"));
		assertEquals("Minor", m.get("severity"));
		assertEquals("Monitored", m.get("action"));
		assertEquals("Content Filter", m.get("detection_type"));
		assertEquals("173.194.126.249", m.get("dst_ip"));
		assertEquals("80", m.get("dst_port"));
		assertEquals("2", m.get("hits"));
		assertEquals("googlesyndication.com", m.get("domain"));
		assertEquals("pagead2.googlesyndication.com/activeview?id=osdim&avi=BHL982oRHU6n7KoKu8AWH1oDQAgDE5Nj4jgEAABABOAHIAQKgBgKoE4AB&adk=164142136&p=33,494,93,962&tos=0,0,0,0,0&mtos=0,0,0,0,0&rs=1&ht=0&fp=client%3Dca-pub-5967860900629716%26url%3Dhttp%253A%252F%252Fwww.82cook", m.get("req_url"));
	
	}
	
	
	@Test
	public void testParser2() {
		String line = "04/14/2014 18:04,10.202.210.173,10.202.210.173,121.78.90.99,Critical Spyware Web Site,Spyware,Critical,"
				+ "Monitored,Malware IP,121.78.90.99,8888: ddi-tcp-1,2,,TCP (10.202.210.173:61942 --> 121.78.90.99:8888)";
		
		SwgLogParser parser = new SwgLogParser();
		Map<String, Object> m = parser.parse(line(line));
		
		Date time = (Date) m.get("timestamp");
		Calendar c = Calendar.getInstance();
		c.setTime(time);
		assertEquals(2014, c.get(Calendar.YEAR));
		assertEquals(3, c.get(Calendar.MONTH));
		assertEquals(14, c.get(Calendar.DAY_OF_MONTH));
		assertEquals(18, c.get(Calendar.HOUR_OF_DAY));
		assertEquals(04, c.get(Calendar.MINUTE));
		
		assertEquals("10.202.210.173", m.get("hostname"));
		assertEquals("10.202.210.173", m.get("local_ip"));
		assertEquals("121.78.90.99", m.get("detection"));
		assertEquals("Critical Spyware Web Site", m.get("category"));
		assertEquals("Spyware", m.get("class"));
		assertEquals("Critical", m.get("severity"));
		assertEquals("Monitored", m.get("action"));
		assertEquals("Malware IP", m.get("detection_type"));
		assertEquals("121.78.90.99", m.get("dst_ip"));
		assertEquals("8888: ddi-tcp-1", m.get("dst_port"));
		assertEquals("2", m.get("hits"));
		assertEquals(null, m.get("domain"));
		assertEquals("TCP (10.202.210.173:61942 --> 121.78.90.99:8888)", m.get("req_url"));
	
	}
	

	private Map<String, Object> line(String line) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);
		return m;
	}
}
