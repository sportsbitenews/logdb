package org.araqne.logdb;

import org.junit.Test;

import static org.junit.Assert.*;

public class TimeSpanTest {

	@Test
	public void testParse(){
		TimeSpan t = TimeSpan.parse("10m");
		assertEquals(10, t.amount);
		assertEquals(TimeUnit.Minute, t.unit);
	}

	@Test
	public void testError90500(){
		String value = "5mon";

		try {
			TimeSpan.parse(value);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println(value);
				System.out.println(e.getMessage());
			}
			assertEquals("90500", e.getType());
			assertEquals(value, e.getParams().get("value"));
		}
	}

	@Test
	public void testError90501(){
		String value = "2y";

		try {
			TimeSpan.parse(value);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println(value);
				System.out.println(e.getMessage());
			}
			assertEquals("90501",  e.getType() );
			assertEquals(value,  e.getParams().get("value"));

		} 
	}

}
