package org.araqne.logdb.query.parser;

import org.araqne.logdb.query.command.BoxPlot;
import org.junit.Test;

import static org.junit.Assert.*;

public class BoxPlotParserTest {
	@Test
	public void testParse() {
		String query = "boxplot long(delay) by cell";
		BoxPlotParser parser = new BoxPlotParser();
		BoxPlot boxPlot = (BoxPlot) parser.parse(null, query);
		assertEquals("cell", boxPlot.getClauses().get(0));
		assertEquals("boxplot long(delay, 10) by cell", boxPlot.toString());
	}
}
