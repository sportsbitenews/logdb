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
package org.araqne.logparser.syslog.riorey;

import static org.junit.Assert.assertEquals;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * @author kyun
 */

public class RioreySysLogParserTest {

	@Test
	public void testParser() {

		String line = "<systemEntry host=\"172.22.7.11\" serial=\"RRPXU211BB101964\" timestamp=\"2014-03-21T00:20:18Z\">"
				+ "<memory><total>8229504</total><free>1196564</free></memory></systemEntry>";

		RioreySyslogParser parser = new RioreySyslogParser();
		Map<String, Object> m = parser.parse(line(line));
		
		assertEquals("172.22.7.11", m.get("host"));
		assertEquals("RRPXU211BB101964", m.get("serial"));
		assertEquals(8229504, m.get("total"));
		assertEquals(1196564, m.get("free"));
		Date time = (Date) m.get("timestamp");
		Calendar c = Calendar.getInstance();
		c.setTime(time);
		assertEquals(2014, c.get(Calendar.YEAR));
		assertEquals(2, c.get(Calendar.MONTH));
		assertEquals(21, c.get(Calendar.DAY_OF_MONTH));
		assertEquals(00, c.get(Calendar.HOUR_OF_DAY));
		assertEquals(20, c.get(Calendar.MINUTE));
		assertEquals(18, c.get(Calendar.SECOND));
		
	}
		
	@Test
	public void testParser2() {

		String line = "<systemEntry host=\"172.22.7.11\" serial=\"RRPXU211BB101964\" timestamp=\"2014-03-21T00:20:18Z\"><CPU><usage>6.0911016</usage></CPU></systemEntry>";
	
		RioreySyslogParser parser = new RioreySyslogParser();
		Map<String, Object> m = parser.parse(line(line));
		
		assertEquals("172.22.7.11", m.get("host"));
		assertEquals("RRPXU211BB101964", m.get("serial"));
		assertEquals("6.0911016", m.get("usage"));
		Date time = (Date) m.get("timestamp");
		Calendar c = Calendar.getInstance();
		c.setTime(time);
		assertEquals(2014, c.get(Calendar.YEAR));
		assertEquals(2, c.get(Calendar.MONTH));
		assertEquals(21, c.get(Calendar.DAY_OF_MONTH));
		assertEquals(00, c.get(Calendar.HOUR_OF_DAY));
		assertEquals(20, c.get(Calendar.MINUTE));
		assertEquals(18, c.get(Calendar.SECOND));
		
	}

	
	@Test
	public void testParser3() {
		String line = "<systemEntry host=\"172.22.7.11\" serial=\"RRPXU211BB101964\" timestamp=\"2014-03-21T00:15:11Z\">"
				+ "<storage><main><total>524288</total><free>207032</free></main><secondary><total>239374</total><free>206133</free></secondary></storage></systemEntry>";
		RioreySyslogParser parser = new RioreySyslogParser();
		Map<String, Object> m = parser.parse(line(line));
		
		assertEquals("172.22.7.11", m.get("host"));
		assertEquals("RRPXU211BB101964", m.get("serial"));
		assertEquals(524288, m.get("main_total"));
		assertEquals(207032, m.get("main_free"));
		assertEquals(239374, m.get("secondary_total"));
		assertEquals(206133, m.get("secondary_free"));
		
		Date time = (Date) m.get("timestamp");
		Calendar c = Calendar.getInstance();
		c.setTime(time);
		assertEquals(2014, c.get(Calendar.YEAR));
		assertEquals(2, c.get(Calendar.MONTH));
		assertEquals(21, c.get(Calendar.DAY_OF_MONTH));
		assertEquals(00, c.get(Calendar.HOUR_OF_DAY));
		assertEquals(15, c.get(Calendar.MINUTE));
		assertEquals(11, c.get(Calendar.SECOND));
		
	}
	
	@Test
	public void testParser4() {
		String line = "<?xml version=\"1.0\" ?><systemEntryList><systemEntry host=\"172.22.7.10\" serial=\"RRPXU211BC102030\" "
				+ "timestamp=\"2014-03-12T21:11:51Z\"><CPU><usage>7.9439564</usage></CPU></systemEntry>";
		
	
		RioreySyslogParser parser = new RioreySyslogParser();
		Map<String, Object> m = parser.parse(line(line));
		
		assertEquals("172.22.7.10", m.get("host"));
		assertEquals("RRPXU211BC102030", m.get("serial"));
		assertEquals("7.9439564", m.get("usage"));
	
		
		Date time = (Date) m.get("timestamp");
		Calendar c = Calendar.getInstance();
		c.setTime(time);
		assertEquals(2014, c.get(Calendar.YEAR));
		assertEquals(2, c.get(Calendar.MONTH));
		assertEquals(12, c.get(Calendar.DAY_OF_MONTH));
		assertEquals(21, c.get(Calendar.HOUR_OF_DAY));
		assertEquals(11, c.get(Calendar.MINUTE));
		assertEquals(51, c.get(Calendar.SECOND));
		
	}
	
	@Test
	public void testParser5() {
		String line = "<systemEntry host=\"172.22.7.10\" serial=\"RRPXU211BC102030\" timestamp=\"2014-03-12T21:11:51Z\">"
				+ "<hardware><fan name=\"Fan_1\"><speed>4556</speed></fan><fan name=\"Fan_2\"><speed>4503</speed></fan>"
				+ "<fan name=\"Fan_3\"><speed>4448</speed></fan><temperature name=\"CPU_1\"><temp>35</temp></temperature>"
				+ "<temperature name=\"CPU_2\"><temp>-128</temp></temperature><temperature name=\"System\"><temp>25</temp></temperature></hardware></systemEntry>";
	
	
		RioreySyslogParser parser = new RioreySyslogParser();
		Map<String, Object> m = parser.parse(line(line));
		
		assertEquals("172.22.7.10", m.get("host"));
		assertEquals("RRPXU211BC102030", m.get("serial"));
		assertEquals(4556, m.get("fan_1_speed"));
		assertEquals(35, m.get("cpu_1_temp"));
		assertEquals(-128, m.get("cpu_2_temp"));
		assertEquals(25, m.get("system_temp"));
		
		Date time = (Date) m.get("timestamp");
		Calendar c = Calendar.getInstance();
		c.setTime(time);
		assertEquals(2014, c.get(Calendar.YEAR));
		assertEquals(2, c.get(Calendar.MONTH));
		assertEquals(12, c.get(Calendar.DAY_OF_MONTH));
		assertEquals(21, c.get(Calendar.HOUR_OF_DAY));
		assertEquals(11, c.get(Calendar.MINUTE));
		assertEquals(51, c.get(Calendar.SECOND));
	}
	
	
	private Map<String, Object> line(String line) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", line);
		return m;
	}
}
