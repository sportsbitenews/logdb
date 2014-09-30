package org.araqne.logdb.query.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.impl.FunctionRegistryImpl;
import org.araqne.logdb.query.command.OutputTxt;
import org.araqne.logdb.query.engine.QueryParserServiceImpl;
import org.junit.Before;
import org.junit.Test;

/**
 * @since 1.6.7
 * @author darkluster
 * 
 */
public class OutputTxtParserTest {
	private QueryParserService queryParserService;

	@Before
	public void setup() {
		QueryParserServiceImpl p = new QueryParserServiceImpl();
		p.setFunctionRegistry(new FunctionRegistryImpl());
		queryParserService = p;
	}

	@Test
	public void testNormalCase() {
		new File("logexport.txt").delete();
		OutputTxt txt = null;
		try {
			OutputTxtParser p = new OutputTxtParser();
			p.setQueryParserService(queryParserService);

			txt = (OutputTxt) p.parse(null, "outputtxt logexport.txt sip, dip ");
			txt.onStart();

			File f = txt.getTxtFile();			
			assertEquals("logexport.txt", f.getName());
			assertEquals("sip", txt.getFields().get(0));
			assertEquals("dip", txt.getFields().get(1));
			assertEquals(" ", txt.getDelimiter());

			assertEquals("outputtxt encoding=utf-8 logexport.txt sip, dip", txt.toString());
		} finally {
			if (txt != null)
				txt.onClose(QueryStopReason.End);
			new File("logexport.txt").delete();
		}
	}

	@Test
	public void testDelimiter() {
		new File("logexport.txt").delete();
		OutputTxt txt = null;
		try {
			OutputTxtParser p = new OutputTxtParser();
			p.setQueryParserService(queryParserService);

			txt = (OutputTxt) p.parse(null, "outputtxt delimiter=\"|\" logexport.txt sip, dip ");
			txt.onStart();

			assertEquals("|", txt.getDelimiter());
			assertEquals("outputtxt encoding=utf-8 delimiter=| logexport.txt sip, dip", txt.toString());
		} finally {
			if (txt != null)
				txt.onClose(QueryStopReason.End);
			new File("logexport.txt").delete();
		}
	}

	@Test
	public void testInvalidDelimiterPosition() {
		new File("logexport.txt").delete();
		OutputTxt txt = null;
		String query =  "outputtxt logexport.txt delimiter=\"|\" sip, dip ";
		try {
			OutputTxtParser p = new OutputTxtParser();
			p.setQueryParserService(queryParserService);

			txt = (OutputTxt) p.parse(null, query);

			assertEquals("|", txt.getDelimiter());
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("90001", e.getType());
			assertEquals(10, e.getOffsetS());
			assertEquals(32, e.getOffsetE());
		} finally {
			new File("logexport.csv").delete();
		}
	} 

	@Test
	public void testError30400() {
		String query = "outputtxt logexport.txt sip,";

		try {
			OutputTxtParser p = new OutputTxtParser();
			p.setQueryParserService(queryParserService);

			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("30400", e.getType());
			assertEquals(27, e.getOffsetS());
			assertEquals(27, e.getOffsetE());	
		}
	}

	@Test
	public void testError30401(){
		String query = "outputtxt {logtime:/yyyy/MM/dd/}{now:HHmm.txt} src_ip, dst_ip";

		try {
			OutputTxtParser p = new OutputTxtParser();
			p.setQueryParserService(queryParserService);

			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("30401", e.getType());
			assertEquals(10, e.getOffsetS());
			assertEquals(60, e.getOffsetE());	
		}
	}
	
	@Test
	public void testError30402() {
		String query = "outputtxt logexport.txt";

		try {
			OutputTxtParser p = new OutputTxtParser();
			p.setQueryParserService(queryParserService);

			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("30402", e.getType());
			assertEquals(10, e.getOffsetS());
			assertEquals(22, e.getOffsetE());	
		}
	}

	@Test
	public void testError30403() throws IOException{
		new File("overwrite").createNewFile();
		String query = "outputtxt overwrite=false tmp=overwrite logexport.txt sip, dip ";
			
		try {
			OutputTxtParser p = new OutputTxtParser();
			p.setQueryParserService(queryParserService);

			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("30403", e.getType());
			assertEquals(30, e.getOffsetS());
			assertEquals(38, e.getOffsetE());	
		}finally{
			new File("overwrite").delete();
		}
	}
	
	@Test
	public void testError30405() {
		String query = "outputtxt";

		try {
			OutputTxtParser p = new OutputTxtParser();
			p.setQueryParserService(queryParserService);

			p.parse(null, query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("30405", e.getType());
			assertEquals(10, e.getOffsetS());
			assertEquals(8, e.getOffsetE());	
		}
	}
}
