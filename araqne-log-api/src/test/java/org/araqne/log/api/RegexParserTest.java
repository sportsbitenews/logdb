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
package org.araqne.log.api;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class RegexParserTest {
	@Test
	public void testParse() {
		RegexParserFactory f = new RegexParserFactory();
		Map<String, String> config = new HashMap<String, String>();
		config.put("field", "line");
		config.put("regex", "^(?<remote_ip>\\S+) \\S+ \\S+ \\[(?<date>[^\\]]+)\\] \"(?<method>[A-Z]+) (?<url>[^\" ]*).*\" "
				+ "(?<status>\\d+) (?<sent>\\d+) \"(?<referer>[^\"]*)\" \"(?<user_agent>[^\"]*)\"$");

		LogParser parser = f.createParser(config);
		Map<String, Object> log = new HashMap<String, Object>();
		log.put("line",
				"11.11.11.11 - - [25/Jan/2000:14:00:01 +0100] \"GET /1986.js HTTP/1.1\" 200 932 \"http://domain.com/index.html\" "
						+ "\"Mozilla/5.0 (Windows; U; Windows NT 5.1; de; rv:1.9.1.7) Gecko/20091221 Firefox/3.5.7 GTB6\"");

		Map<String, Object> parsed = parser.parse(log);
		assertEquals("932", parsed.get("sent"));
		assertEquals("200", parsed.get("status"));
		assertEquals("11.11.11.11", parsed.get("remote_ip"));
		assertEquals("25/Jan/2000:14:00:01 +0100", parsed.get("date"));
		assertEquals("Mozilla/5.0 (Windows; U; Windows NT 5.1; de; rv:1.9.1.7) Gecko/20091221 Firefox/3.5.7 GTB6",
				parsed.get("user_agent"));
		assertEquals("GET", parsed.get("method"));
		assertEquals("http://domain.com/index.html", parsed.get("referer"));
		assertEquals("/1986.js", parsed.get("url"));
	}
	
	// related with araqne/issue#280
	@Test
	public void testNestedRegex() {
		RegexParserFactory f = new RegexParserFactory();
		Map<String, String> config = new HashMap<String, String>();
		config.put("field", "line");
		config.put("regex", "(?<payload>cpu_usage=\\\"(?<cpu_usage>.*)\\\" mem_usage=\\\"(?<mem_usage>.*)\\\")");

		LogParser parser = f.createParser(config);
		Map<String, Object> log = new HashMap<String, Object>();
		log.put("line","sample cpu_usage=\"3 %\" mem_usage=\"60 %\"");

		Map<String, Object> parsed = parser.parse(log);

		assertEquals("3 %", parsed.get("cpu_usage"));
		assertEquals("60 %", parsed.get("mem_usage"));
		assertEquals("cpu_usage=\"3 %\" mem_usage=\"60 %\"", parsed.get("payload"));
		
		config.put("regex", "((?<key>\\w+,\\w+))");
		parser = f.createParser(config);
		
		log.put("line", "sample (aaa,bbb)");
		
		parsed = parser.parse(log);

		assertEquals("aaa,bbb", parsed.get("key"));

		config.put("regex", "\\((?<key>\\w+,\\w+)\\)");
		parser = f.createParser(config);
		
		log.put("line", "sample (aaa,bbb)");
		
		parsed = parser.parse(log);

		assertEquals("aaa,bbb", parsed.get("key"));

	}
}
