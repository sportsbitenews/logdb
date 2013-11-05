/*
 * Copyright 2013 Future Systems
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
package org.araqne.logdb.query.parser;

import org.araqne.logdb.BaseLogQueryScript;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.LogQueryScriptRegistry;
import org.araqne.logdb.query.command.Script;
import org.junit.Test;
import org.osgi.framework.BundleContext;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

public class ScriptParserTest {
	@Test
	public void testSimpleQuery() {
		MockQueryScript mockScript = new MockQueryScript();

		BundleContext bc = null;
		LogQueryScriptRegistry scriptRegistry = mock(LogQueryScriptRegistry.class);
		when(scriptRegistry.newScript("localhost", "sample", null)).thenReturn(mockScript);

		ScriptParser parser = new ScriptParser(bc, scriptRegistry);
		LogQueryContext context = new LogQueryContext(null);

		Script script = (Script) parser.parse(context, "script sample");
		assertEquals(mockScript, script.getQueryScript());
		assertEquals("sample", script.getScriptName());
		assertEquals("script sample", script.toString());
	}

	@Test
	public void testScriptQueryWithArgs() {
		MockQueryScript mockScript = new MockQueryScript();

		BundleContext bc = null;
		LogQueryScriptRegistry scriptRegistry = mock(LogQueryScriptRegistry.class);
		when(scriptRegistry.newScript("localhost", "sample", null)).thenReturn(mockScript);

		ScriptParser parser = new ScriptParser(bc, scriptRegistry);
		LogQueryContext context = new LogQueryContext(null);

		Script script = (Script) parser.parse(context, "script key1=value1 key2=value2 sample");
		assertEquals(mockScript, script.getQueryScript());
		assertEquals("sample", script.getScriptName());
		assertEquals(2, script.getParameters().size());
		assertEquals("value1", script.getParameters().get("key1"));
		assertEquals("value2", script.getParameters().get("key2"));
		assertEquals("script key2=value2 key1=value1 sample", script.toString());
	}

	private static class MockQueryScript extends BaseLogQueryScript {
	}
}
