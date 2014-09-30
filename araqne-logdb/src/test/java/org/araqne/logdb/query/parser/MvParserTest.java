package org.araqne.logdb.query.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.impl.FunctionRegistryImpl;
import org.araqne.logdb.query.engine.QueryParserServiceImpl;
import org.junit.Before;
import org.junit.Test;


public class MvParserTest {
	private QueryParserService queryParserService;

	@Before
	public void setup() {
		QueryParserServiceImpl p = new QueryParserServiceImpl();
		p.setFunctionRegistry(new FunctionRegistryImpl());
		queryParserService = p;
	}

	@Test
	public void testError30500() throws IOException{
		String query = "mv from=a.txt, ";
			
		try {
			MvParser p = new MvParser();
			p.setQueryParserService(queryParserService);

			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("30500", e.getType());
			assertEquals(13, e.getOffsetS());
			assertEquals(13, e.getOffsetE());	
		}
	}
	
	@Test
	public void testError30501() throws IOException{
		String query = "mv from=a.txt";
			
		try {
			MvParser p = new MvParser();
			p.setQueryParserService(queryParserService);

			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("30501", e.getType());
			assertEquals(3, e.getOffsetS());
			assertEquals(12, e.getOffsetE());	
		}finally{
			new File("tmpfile.txt").delete();
		}
	}
	
	
	@Test
	public void testError30502() throws IOException{
		new File("tofile.txt").createNewFile();
		String query = "mv from=tofile.txt to=tofile.txt";
			
		try {
			MvParser p = new MvParser();
			p.setQueryParserService(queryParserService);

			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("30502", e.getType());
			assertEquals(22, e.getOffsetS());
			assertEquals(31, e.getOffsetE());	
		}finally{
			new File("tmpfile.txt").delete();
		}
	}
	
	
}
