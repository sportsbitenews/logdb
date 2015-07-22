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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.araqne.logdb.Account;
import org.araqne.logdb.AccountService;
import org.araqne.logdb.Procedure;
import org.araqne.logdb.ProcedureParameter;
import org.araqne.logdb.ProcedureRegistry;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.impl.FunctionRegistryImpl;
import org.araqne.logdb.query.command.Proc;
import org.araqne.logdb.query.engine.QueryParserServiceImpl;
import org.junit.Before;
import org.junit.Test;


public class ProcParserTest {
	private QueryParserService queryParserService;

	@Before
	public void setup() {
		QueryParserServiceImpl p = new QueryParserServiceImpl();
		p.setFunctionRegistry(new FunctionRegistryImpl());
		queryParserService = p;
	}

	@Test
	public void testError11000() {
		ProcParser p = new ProcParser(null, queryParserService, null);
		p.setQueryParserService(queryParserService); 
		String query = "proc undefined()";	

		try {
			parse(query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("11000", e.getType());
			assertEquals(5, e.getStartOffset());
			assertEquals(15, e.getEndOffset());
		}
	}

	@Test
	public void testError11001(){
		ProcParser p = new ProcParser(null, queryParserService, null);
		p.setQueryParserService(queryParserService); 
		String query = "proc tables(\"log\", 1.1)";	

		try {
			parse(query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("11001", e.getType());
			assertEquals(11, e.getStartOffset());
			assertEquals(22, e.getEndOffset());
		}
	}

	@Test
	public void testError11002(){
		ProcParser p = new ProcParser(null, queryParserService, null);
		p.setQueryParserService(queryParserService); 
		String query = "proc tables(\"log\", 1)";	

		try {
			parse(query);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("11002", e.getType());
			assertEquals(-1, e.getStartOffset());
			assertEquals(-1, e.getEndOffset());
		}
	}


	@Test
	public void testError11003(){
		ProcParser p = new ProcParser(null, queryParserService, null);
		p.setQueryParserService(queryParserService); 
		String query = "proc tables(\"log\", 1, 2)";	

		try {
			parse(query, true);
			fail();
		} catch (QueryParseException e) {
			if(e.isDebugMode()){
				System.out.println("query " + query);
				System.out.println(e.getMessage());
			}
			assertEquals("11003", e.getType());
			assertEquals(5, e.getStartOffset());
			assertEquals(23, e.getEndOffset());
		}
	}

	private Proc parse(String query){
		return parse(query, false);
	}

	private Proc parse(String query, boolean isAccount) {
		List<ProcedureParameter> list = new ArrayList<ProcedureParameter>();
		ProcedureParameter stringParam = new ProcedureParameter();
		ProcedureParameter intParam = new ProcedureParameter();
		stringParam.setType("string");
		intParam.setType("int");
		list.add(stringParam);
		list.add(intParam);

		AccountService mockAccount = mock(AccountService.class);
		ProcedureRegistry mockParserRegistry = mock(ProcedureRegistry.class);
		Procedure mockProcedure = mock(Procedure.class);

		when(mockParserRegistry.getProcedure("tables")).thenReturn(mockProcedure);
		when(mockProcedure.getParameters()).thenReturn(list);
		when(mockProcedure.getName()).thenReturn("tables");
		if(isAccount) 
			when(mockAccount.getAccount(null)).thenReturn(new Account());

		ProcParser parser = new ProcParser(mockAccount,  null , mockParserRegistry);
		parser.setQueryParserService(queryParserService);
		Proc proc = (Proc) parser.parse(new QueryContext(null), query);
		return proc;
	}
}
