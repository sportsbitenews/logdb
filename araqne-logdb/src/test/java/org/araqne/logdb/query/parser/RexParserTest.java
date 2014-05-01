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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryCommandPipe;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.Row;
import org.araqne.logdb.impl.FunctionRegistryImpl;
import org.araqne.logdb.query.command.Rex;
import org.araqne.logdb.query.engine.QueryParserServiceImpl;
import org.junit.Before;
import org.junit.Test;

public class RexParserTest {
	private QueryParserService queryParserService;

	@Before
	public void setup() {
		QueryParserServiceImpl p = new QueryParserServiceImpl();
		p.setFunctionRegistry(new FunctionRegistryImpl());
		queryParserService = p;
	}
	
	@Test
	public void testRexItself() {
		Pattern p = Pattern.compile("From: (.*) To: (.*)");
		String[] names = new String[] { "from", "to" };

		DummyOutput out = new DummyOutput();
		Rex rex = new Rex("raw", "", p, names);
		rex.setOutput(new QueryCommandPipe(out));

		Row m = new Row();
		m.put("raw", "From: Susan To: Bob");

		rex.onPush(m);

		assertEquals(1, out.list.size());
		Row m2 = out.list.get(0);

		assertEquals("From: Susan To: Bob", (String) m2.get("raw"));
		assertEquals("Susan", (String) m2.get("from"));
		assertEquals("Bob", (String) m2.get("to"));
	}

	private static class DummyOutput extends QueryCommand {
		private List<Row> list = new ArrayList<Row>();

		@Override
		public String getName() {
			return "output";
		}

		@Override
		public void onPush(Row m) {
			list.add(m);
		}

		@Override
		public boolean isReducer() {
			return false;
		}
	}

	@Test
	public void testRexCommandParse() {
		RexParser parser = new RexParser();
		parser.setQueryParserService(queryParserService);
		
		Rex rex = (Rex) parser.parse(null, "rex field=_raw \"From: (?<from>.*) To: (?<to>.*)\"");

		assertEquals("_raw", rex.getInputField());
		assertEquals("from", rex.getOutputNames()[0]);
		assertEquals("to", rex.getOutputNames()[1]);
		assertEquals("From: (.*) To: (.*)", rex.getPattern().toString());
	}

	@Test
	public void testRexQueryGeneration() {
		String QUERY = "rex field=_raw \"From: (?<from>.*) To: (?<to>.*)\"";
		RexParser parser = new RexParser();
		parser.setQueryParserService(queryParserService);
		
		Rex rex = (Rex) parser.parse(null, QUERY);
		String query = rex.toString();
		assertEquals(QUERY, query);
	}

	@Test
	public void testRexCommandParse2() {
		String s = "Close[00:00:20. SF. FIN] NAT[313]  R[16]";
		Row map = new Row();
		map.put("note", s);

		RexParser parser = new RexParser();
		parser.setQueryParserService(queryParserService);
		
		Rex rex = (Rex) parser.parse(null, "rex field=note \"N(A|B)T\\[(?<nat>.*?)\\]  R\\[(?<r>.*?)\\]\"");
		DummyOutput out = new DummyOutput();
		rex.setOutput(new QueryCommandPipe(out));

		rex.onPush(map);
		Row map2 = out.list.get(0);

		assertEquals("313", map2.get("nat"));
		assertEquals("16", map2.get("r"));
	}

	@Test
	public void testEscape() {
		String s = "rex field=line \"(?<d>\\d+-\\d+-\\d+)\"";
		RexParser parser = new RexParser();
		parser.setQueryParserService(queryParserService);
		
		Rex rex = (Rex) parser.parse(null, s);
		assertEquals("(\\d+-\\d+-\\d+)", rex.getPattern().toString());
	}

	@Test
	public void testEscapeQueryGeneration() {
		String s = "rex field=line \"(?<d>\\d+-\\d+-\\d+)\" ";
		RexParser parser = new RexParser();
		parser.setQueryParserService(queryParserService);
		
		Rex rex = (Rex) parser.parse(null, s);
		String query = rex.toString();
		assertEquals(s, query);
	}
	
	// for araqne/issue#280
	@Test
	public void testNestedCapture() {
		String s = "rex field=line \"(?<payload>cpu_usage=\\\"(?<cpu_usage>.*)\\\" mem_usage=\\\"(?<mem_usage>.*)\\\")\"";
		RexParser parser = new RexParser();
		parser.setQueryParserService(queryParserService);
		
		Rex rex = (Rex) parser.parse(null, s);

		DummyOutput out = new DummyOutput();
		rex.setOutput(new QueryCommandPipe(out));
		Row map = new Row();
		map.put("line", "sample cpu_usage=\"3 %\" mem_usage=\"60 %\"");

		rex.onPush(map);
		Row map2 = out.list.get(0);

		assertEquals("3 %", map2.get("cpu_usage"));
		assertEquals("60 %", map2.get("mem_usage"));
		assertEquals("cpu_usage=\"3 %\" mem_usage=\"60 %\"", map2.get("payload"));
		
		s = "rex field=line \"((?<key>\\w+,\\w+))\"";
		parser = new RexParser();
		parser.setQueryParserService(queryParserService);
		
		rex = (Rex) parser.parse(null, s);
		
		out = new DummyOutput();
		rex.setOutput(new QueryCommandPipe(out));
		map = new Row();
		map.put("line", "sample (aaa,bbb)");
		
		rex.onPush(map);
		map2 = out.list.get(0);
		
		assertEquals("aaa,bbb", map2.get("key"));

		s = "rex field=line \"\\((?<key>\\w+,\\w+)\\)\"";
		parser = new RexParser();
		parser.setQueryParserService(queryParserService);
		
		rex = (Rex) parser.parse(null, s);
		
		out = new DummyOutput();
		rex.setOutput(new QueryCommandPipe(out));
		map = new Row();
		map.put("line", "sample (aaa,bbb)");
		
		rex.onPush(map);
		map2 = out.list.get(0);
		
		assertEquals("aaa,bbb", map2.get("key"));
	}
	

	// for araqne/issue#127
	@Test
	public void testIgnoreInnerKeyValueOptionPattern() {
		String s = "rex field=line \"cpu_usage=\\\"(?<cpu_usage>.*)\\\" mem_usage=\\\"(?<mem_usage>.*)\\\"\"";
		RexParser parser = new RexParser();
		parser.setQueryParserService(queryParserService);
		
		Rex rex = (Rex) parser.parse(null, s);

		// Note that escape-quote sequence is preserved, it is intended result
		// for query convenience.
		assertEquals("cpu_usage=\\\"(.*)\\\" mem_usage=\\\"(.*)\\\"", rex.getPattern().toString());

		DummyOutput out = new DummyOutput();
		rex.setOutput(new QueryCommandPipe(out));
		Row map = new Row();
		map.put("line", "sample cpu_usage=\"3 %\" mem_usage=\"60 %\"");

		rex.onPush(map);
		Row map2 = out.list.get(0);

		assertEquals("3 %", map2.get("cpu_usage"));
		assertEquals("60 %", map2.get("mem_usage"));
	}

	@Test
	public void testInvalidOptionBug() {
		String s = "rex field=line \"\\d+\\s+(?<xtime>\\d+-\\d+-\\d+\\w+\\d+:\\d+:\\d+\\.\\d+)[-\\w\\d].+RT_FLOW\\s+-\\s+(?<xstatus>\\w+)\\s+[-@=\\[\\w\\d\\\"\\.\\s].+\\s+"
				+ "source-address=\\\"(?<s_info>[\\.\\d]+)\\\"\\s+source-port=\\\"(?<s_port>\\d+)\\\"\\s+destination-address=\\\"(?<d_info>[\\.\\d]+)\\\"\\s+"
				+ "destination-port=\\\"(?<d_port>\\d+)\\\"\\s+(?<ext5>[-=\\w\\\"].+?)(\\s+nat-source-address=\\\"(?<xext1_ip>[\\.\\d]+)\\\"\\s+nat-source-port=\\\"(?<xext1_port>[\\d]+)\\\"\\s+"
				+ "nat-destination-address=\\\"(?<xext2_ip>[\\.\\d]+)\\\"\\s+nat-destination-port=\\\"(?<xext2_port>[\\d]+)\\\"\\s+(?<xnote1>[-=\\w\\s\\\"].+)\\s+|\\s+)protocol-id=\\\"(?<protocol>\\d+)\\\"+(\\s+|[-\\s\\w=\\d\\\"].+)"
				+ "policy-name=\\\"(?<ext4>\\d+)\\\"+\\s+(?<xnote2>[-=\\w\\s\\\"].+?)\\s++(?<xnote3>[-=\\w\\s\\\"].+?)\\s+(session-id-32=\\\"(?<user_id>\\d+)\\\"\\s|\\s*)(?<xnote4>[-=\\w\\s\\\"].+)\\]\" ";

		RexParser parser = new RexParser();
		parser.setQueryParserService(queryParserService);
		
		Rex rex = (Rex) parser.parse(null, s);
		assertEquals("line", rex.getInputField());
	}
}
