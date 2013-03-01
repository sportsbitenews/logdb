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

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import static org.junit.Assert.*;

public class DelimiterParserTest {
	@Test
	public void testParse() {
		String s = "1;2;3;4;5";
		DelimiterParser p = new DelimiterParser(";", new String[] { "a", "b", "c", "d", "e" });
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", s);
		Map<String, Object> parsed = p.parse(m);

		assertEquals("1", parsed.get("a"));
		assertEquals("2", parsed.get("b"));
		assertEquals("3", parsed.get("c"));
		assertEquals("4", parsed.get("d"));
		assertEquals("5", parsed.get("e"));

	}

	@Test
	public void testDelimiterRepeat() {
		// default java StringTokenizer cannot handle this case correctly
		String s = ";1;2;;";
		DelimiterParser p = new DelimiterParser(";", new String[] { "a", "b", "c", "d", "e" });
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("line", s);
		Map<String, Object> parsed = p.parse(m);

		assertNull(parsed.get("a"));
		assertEquals("1", parsed.get("b"));
		assertEquals("2", parsed.get("c"));
		assertNull(parsed.get("d"));
		assertNull(parsed.get("e"));
	}
}
