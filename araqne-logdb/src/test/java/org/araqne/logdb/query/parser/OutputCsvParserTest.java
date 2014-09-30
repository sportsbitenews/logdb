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
import static org.junit.Assert.fail;

import java.io.File;

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.impl.FunctionRegistryImpl;
import org.araqne.logdb.query.command.OutputCsv;
import org.araqne.logdb.query.engine.QueryParserServiceImpl;
import org.junit.Before;
import org.junit.Test;

public class OutputCsvParserTest {
	private QueryParserService queryParserService;

	@Before
	public void setup() {
		QueryParserServiceImpl p = new QueryParserServiceImpl();
		p.setFunctionRegistry(new FunctionRegistryImpl());
		queryParserService = p;
	}
	
	@Test
	public void testNormalCase() {
		new File("logexport.csv").delete();
		OutputCsv csv = null;
		try {
			OutputCsvParser p = new OutputCsvParser();
			p.setQueryParserService(queryParserService);
			
			csv = (OutputCsv) p.parse(null, "outputcsv logexport.csv sip, dip ");
			csv.onStart();
			
			File f = csv.getCsvFile();
			assertEquals("logexport.csv", f.getName());
			assertEquals("sip", csv.getFields().get(0));
			assertEquals("dip", csv.getFields().get(1));

		} finally {
			// to close csv file handle
			if (csv != null)
				csv.onClose(QueryStopReason.End);
			new File("logexport.csv").delete();
		}
	}

	@Test
	public void testOverwriteQueryGeneration() {
		new File("logexport.csv").delete();
		OutputCsv csv = null;
		try {
			OutputCsvParser p = new OutputCsvParser();
			p.setQueryParserService(queryParserService);
			
			csv = (OutputCsv) p.parse(null, "outputcsv overwrite=true logexport.csv sip, dip ");
			csv.onStart();
			
			File f = csv.getCsvFile();
			assertEquals("logexport.csv", f.getName());
			assertEquals("sip", csv.getFields().get(0));
			assertEquals("dip", csv.getFields().get(1));

		} finally {
			// to close csv file handle
			if (csv != null)
				csv.onClose(QueryStopReason.End);
			new File("logexport.csv").delete();
		}

		assertEquals("outputcsv overwrite=true encoding=utf-8 logexport.csv sip, dip", csv.toString());
	}
	
	@Test
	public void testError30100() {
		String query= "outputcsv logexport.csv,";
		
		try {
			OutputCsvParser p = new OutputCsvParser();
			p.setQueryParserService(queryParserService);
			
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("30200", e.getType());
			assertEquals(23, e.getOffsetS());
			assertEquals(23, e.getOffsetE());	
		} 
	}

	@Test
	public void testMissingField1() {
		String query= "outputcsv logexport.csv";
		
		try {
			OutputCsvParser p = new OutputCsvParser();
			p.setQueryParserService(queryParserService);
			
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("30202", e.getType());
			assertEquals(10, e.getOffsetS());
			assertEquals(22, e.getOffsetE());	
		}
	}

	@Test
	public void testMissingField2() {
		String query = "outputcsv {logtime:/yyyy/MM/dd/}{now:HHmm.csv} src_ip, dst_ip";
		
		try {
			OutputCsvParser p = new OutputCsvParser();
			p.setQueryParserService(queryParserService);
			
			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("30201", e.getType());
			assertEquals(10, e.getOffsetS());
			assertEquals(60, e.getOffsetE());	
		}
	}
}
