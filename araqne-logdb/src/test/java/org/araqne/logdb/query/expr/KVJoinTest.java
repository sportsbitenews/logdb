/**
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
package org.araqne.logdb.query.expr;

import static org.junit.Assert.*;

import java.util.Map;
import java.util.TreeMap;

import org.araqne.logdb.Row;
import org.junit.Test;

public class KVJoinTest {
	@Test
	public void testManual() {
		{
			Map<String, Object> map = new TreeMap<String, Object>();
			map.put("src", "10.0.0.1");
			map.put("srd", "10.0.0.2");
			map.put("dst", "10.0.0.10");
			assertEquals("dst:10.0.0.10^^src:10.0.0.1^^srd:10.0.0.2", FunctionUtil.parseExpr("kvjoin(\":\", \"^^\")").eval(new Row(map)));
		}

		{
			Map<String, Object> map = new TreeMap<String, Object>();
			map.put("src", "10.0.0.1");
			map.put("srd", "10.0.0.2");
			map.put("dst", "10.0.0.10");
			assertEquals("src:10.0.0.1^^srd:10.0.0.2", FunctionUtil.parseExpr("kvjoin(\":\", \"^^\",\"sr.*\")").eval(new Row(map)));
		}
	}

}
