package org.araqne.logdb.query.parser;

import static org.junit.Assert.assertEquals;

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
}
