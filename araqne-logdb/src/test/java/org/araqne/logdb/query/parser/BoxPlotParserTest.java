package org.araqne.logdb.query.parser;

import static org.junit.Assert.*;

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.impl.FunctionRegistryImpl;
import org.araqne.logdb.query.command.BoxPlot;
import org.araqne.logdb.query.engine.QueryParserServiceImpl;
import org.junit.Before;
import org.junit.Test;

public class BoxPlotParserTest {
	private QueryParserService queryParserService;
	
	@Before
	public void setup() {
		QueryParserServiceImpl p = new QueryParserServiceImpl();
		p.setFunctionRegistry(new FunctionRegistryImpl());
		queryParserService = p;
	}
	
	@Test
	public void testParse() {
		String query = "boxplot long(delay) by cell";
		BoxPlotParser parser = new BoxPlotParser();
		parser.setQueryParserService(queryParserService);
		
		BoxPlot boxPlot = (BoxPlot) parser.parse(null, query);
		assertEquals("cell", boxPlot.getClauses().get(0));
		assertEquals("boxplot long(delay, 10) by cell", boxPlot.toString());
	}
	
	@Test
	public void testBoxPlotErr20000(){
		String query = "boxplot long(delay) by cell,";
		BoxPlotParser parser = new BoxPlotParser();
		
		try {
			parser.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("20000", e.getType());
			assertEquals(27, e.getOffsetS());
			assertEquals(27, e.getOffsetE());
		}
	}
	
	@Test
	public void testBoxPlotErr20001(){
		String query = "boxplot by cell";
		BoxPlotParser parser = new BoxPlotParser();
		parser.setQueryParserService(queryParserService);

		try {
			parser.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("20001", e.getType());
			//assertEquals(8, e.getErrStart());
			//assertEquals(8, e.getErrEnd());
		}
		
		query = "boxplot";
		try {
			parser.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("20001", e.getType());
			assertEquals(8, e.getOffsetS());
			assertEquals(6, e.getOffsetE());
		}
	}
}


