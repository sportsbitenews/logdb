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
package org.araqne.logdb.query.command;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.araqne.logdb.impl.XmlParser;
import org.junit.Test;

/**
 * @author xeraph
 */
public class ParseXmlTest {
	@SuppressWarnings("unchecked")
	@Test
	public void testWindowsEventXml() throws Throwable {
		String xml = "<Event xmlns='http://schemas.microsoft.com/win/2004/08/events/event'>"
				+ "<System><Provider Name='Microsoft-Windows-WPDClassInstaller' Guid='{AD5162D8-DAF0-4A25-88A7-01CBEB33902E}'/>"
				+ "<EventID>200</EventID><Version>0</Version><Level>4</Level><Task>1</Task><Opcode>0</Opcode><Keywords>0x8000000000000000</Keywords>"
				+ "<TimeCreated SystemTime='2015-03-22T07:41:44.474069100Z'/><EventRecordID>8</EventRecordID><Correlation/>"
				+ "<Execution ProcessID='2816' ThreadID='10780'/><Channel>Microsoft-Windows-WPD-ClassInstaller/Operational</Channel>"
				+ "<Computer>xeraph-laptop.hq.eediom.net</Computer><Security UserID='S-1-5-18'/></System><EventData></EventData></Event>";

		Map<String, Object> m = XmlParser.parseXml(xml);

		Map<String, Object> system = (Map<String, Object>) m.get("System");
		assertEquals("8", system.get("EventRecordID"));
		assertEquals("200", system.get("EventID"));
		assertEquals("1", system.get("Task"));

		Map<String, Object> provider = (Map<String, Object>) system.get("Provider");
		assertEquals("Microsoft-Windows-WPDClassInstaller", provider.get("Name"));
		assertEquals("{AD5162D8-DAF0-4A25-88A7-01CBEB33902E}", provider.get("Guid"));
	}
}
