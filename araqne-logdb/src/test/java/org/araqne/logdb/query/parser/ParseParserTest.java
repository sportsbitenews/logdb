package org.araqne.logdb.query.parser;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import org.araqne.log.api.LogParser;
import org.araqne.log.api.LogParserProfile;
import org.araqne.log.api.LogParserRegistry;
import org.araqne.logdb.FunctionRegistry;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParserService;
import org.junit.Test;

public class ParseParserTest {
	@Test
	public void parseSuccessTest() {
		assertEquals("parse test", getParseResult("parse test"));

		assertEquals("parse \"[Test: *]\" as test, \"[Time: *]\" as time", 
				getParseResult("parse \"[Test: *]\" as test, \"[Time: *]\" as time"));
	}

	@Test
	public void parseFailureTest() {
		// assertEquals("parse test", getParseResult("parse test"));
	}

	private String getParseResult(String testString) {
		ParseParser pp = new ParseParser(mockLogParserRegistry());

		pp.setQueryParserService(mockQueryParserService());

		QueryCommand p1 = pp.parse(mockQueryContext(), testString);

		return p1.toString();
	}

	private QueryParserService mockQueryParserService() {
		QueryParserService qps = mock(QueryParserService.class);
		when(qps.getFunctionRegistry()).thenReturn(mockFunctionRegistry());

		return qps;
	}

	private FunctionRegistry mockFunctionRegistry() {
		FunctionRegistry fr = mock(FunctionRegistry.class);

		return fr;
	}

	private LogParserRegistry mockLogParserRegistry() {
		LogParserRegistry lpr = mock(LogParserRegistry.class);

		when(lpr.getProfile("test")).thenReturn(mock(LogParserProfile.class));
		when(lpr.newParser("test")).thenReturn(mock(LogParser.class));

		return lpr;
	}

	private QueryContext mockQueryContext() {
		QueryContext ret = mock(QueryContext.class);

		return ret;
	}
}
