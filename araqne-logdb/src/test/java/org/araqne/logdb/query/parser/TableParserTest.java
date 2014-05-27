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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.araqne.log.api.LogParserFactoryRegistry;
import org.araqne.log.api.LogParserRegistry;
import org.araqne.logdb.AccountService;
import org.araqne.logdb.Permission;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryParserService;
import org.araqne.logdb.impl.FunctionRegistryImpl;
import org.araqne.logdb.query.command.StorageObjectName;
import org.araqne.logdb.query.command.Table;
import org.araqne.logdb.query.engine.QueryParserServiceImpl;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogStorageStatus;
import org.araqne.logstorage.LogTableRegistry;
import org.araqne.logstorage.TableSchema;
import org.junit.Before;
import org.junit.Test;

public class TableParserTest {
	private QueryParserService queryParserService;

	@Before
	public void setup() {
		QueryParserServiceImpl p = new QueryParserServiceImpl();
		p.setFunctionRegistry(new FunctionRegistryImpl());
		queryParserService = p;
	}
	
	@Test
	public void testSimpleCase() {
		String query = "table iis";
		Table table = parse(query);

		assertTrue(table.getTableNames().contains("iis"));
		assertEquals(0, table.getOffset());
		assertEquals(0, table.getLimit());
	}

	@Test
	public void testMultiTables() {
		String query = "table iis, xtm";
		Table table = parse(query);

		System.out.println(table.getTableNames());
		assertTrue(table.getTableNames().contains("iis"));
		assertTrue(table.getTableNames().contains("xtm"));
	}

	@Test
	public void testOffsetLimit() {
		String query = "table offset=3 limit=10 iis";
		Table table = parse(query);

		assertTrue(table.getTableNames().contains("iis"));
		assertEquals(3, table.getOffset());
		assertEquals(10, table.getLimit());
	}

	@Test
	public void testMinusOffset() {
		String query = "table offset=-3 iis";

		try {
			parse(query);
			fail();
		} catch (QueryParseException e) {
			assertEquals("negative-offset", e.getType());
		}

	}

	@Test
	public void testMinusLimit() {
		String query = "table limit=-11 iis";

		try {
			parse(query);
			fail();
		} catch (QueryParseException e) {
			assertEquals("negative-limit", e.getType());
		}
	}

	@Test
	public void testFromTo() {
		// yyyyMMdd
		String query = "table from=20120101 to=20121225 iis";
		Table table = parse(query);

		assertEquals(date(2012, 1, 1, 0, 0, 0), table.getFrom());
		assertEquals(date(2012, 12, 25, 0, 0, 0), table.getTo());

		// yyyyMMddHH
		query = "table from=2012010103 to=2012122515 iis";
		table = parse(query);

		assertEquals(date(2012, 1, 1, 3, 0, 0), table.getFrom());
		assertEquals(date(2012, 12, 25, 15, 0, 0), table.getTo());

		// yyyyMMddHHmm
		query = "table from=201201010311 to=201212251544 iis";
		table = parse(query);

		assertEquals(date(2012, 1, 1, 3, 11, 0), table.getFrom());
		assertEquals(date(2012, 12, 25, 15, 44, 0), table.getTo());

		// yyyyMMddHHmmss
		query = "table from=20120101031101 to=20121225154459 iis";
		table = parse(query);

		assertTrue(table.getTableNames().contains("iis"));
		assertEquals(date(2012, 1, 1, 3, 11, 1), table.getFrom());
		assertEquals(date(2012, 12, 25, 15, 44, 59), table.getTo());
	}

	private Date date(int year, int month, int day, int hour, int min, int sec) {
		Calendar c = Calendar.getInstance();
		c.set(Calendar.YEAR, year);
		c.set(Calendar.MONTH, month - 1);
		c.set(Calendar.DAY_OF_MONTH, day);
		c.set(Calendar.HOUR_OF_DAY, hour);
		c.set(Calendar.MINUTE, min);
		c.set(Calendar.SECOND, sec);
		c.set(Calendar.MILLISECOND, 0);
		return c.getTime();
	}

	@Test
	public void testDuration() {
		long baseline = System.currentTimeMillis();
		String query = "table duration=1d iis";
		Table t = (Table) parse(query);
		long from = t.getFrom().getTime();
		long duration = baseline - from;

		// assume that parse() do not consume more than 1sec
		assertTrue((duration - 86400000) <= 1);
	}

	@Test
	public void testMeta() {
		String query = "table duration=1d meta(\"logparser==trusguard\"), iis";
		Table table = parse(query);

		System.out.println(table.getTableNames());
		assertTrue(table.getTableNames().contains("meta(\"logparser==trusguard\", *)"));
		assertTrue(table.getTableNames().contains("iis"));

		query = "table duration=1d meta(\"logparser==trusguard\", \"n1:s*\", \"*:s2\", \"s3\"), iis";
		table = parse(query);

		System.out.println(table.getTableNames());
		assertEquals("meta(\"logparser==trusguard\", n1:s*)", table.getTableSpecs().get(0).toString());
		assertEquals("meta(\"logparser==trusguard\", *:s2)", table.getTableSpecs().get(1).toString());
		assertEquals("meta(\"logparser==trusguard\", s3)", table.getTableSpecs().get(2).toString());
		assertEquals("iis", table.getTableSpecs().get(3).toString());

		assertEquals("n1", table.getTableSpecs().get(0).getNamespace());
		assertEquals("*", table.getTableSpecs().get(1).getNamespace());
		assertEquals(null, table.getTableSpecs().get(2).getNamespace());
		assertEquals(null, table.getTableSpecs().get(3).getNamespace());
	}

	private TableSchema mockSchema(String tableName, String logParser) {
		TableSchema mock = new TableSchema();
		mock.setName(tableName);
		mock.getMetadata().put("logparser", logParser);
		return mock;
	}

	@Test
	public void testOptional() {
		LogTableRegistry mockTableRegistry = mock(LogTableRegistry.class);
		when(mockTableRegistry.exists("xtm")).thenReturn(true);
		when(mockTableRegistry.getTableNames()).thenReturn(Arrays.asList("xtm"));
		when(mockTableRegistry.getTableSchema("xtm", true)).thenReturn(mockSchema("xtm", "trusguard"));

		String query = "table duration=1d meta(\"logparser==trusguard\"), iis?";
		Table table = parse(query);

		System.out.println(table.getTableNames());
		assertTrue(table.getTableNames().contains("meta(\"logparser==trusguard\", *)"));
		assertTrue(table.getTableNames().contains("iis?"));

		{
			List<StorageObjectName> names = new ArrayList<StorageObjectName>();
			for (TableSpec spec : table.getTableSpecs()) {
				names.addAll(spec.match(mockTableRegistry));
			}
			assertTrue(!names.contains(new StorageObjectName(null, "iis", false)));
		}
		when(mockTableRegistry.exists("xtm")).thenReturn(true);
		when(mockTableRegistry.exists("iis")).thenReturn(true);
		when(mockTableRegistry.getTableNames()).thenReturn(Arrays.asList("iis", "xtm"));
		when(mockTableRegistry.getTableSchema("iis", true)).thenReturn(mockSchema("iis", null));
		when(mockTableRegistry.getTableSchema("xtm", true)).thenReturn(mockSchema("xtm", "trusguard"));
		{
			List<StorageObjectName> names = new ArrayList<StorageObjectName>();
			for (TableSpec spec : table.getTableSpecs()) {
				names.addAll(spec.match(mockTableRegistry));
			}
			assertTrue(names.contains(new StorageObjectName(null, "iis", true)));
		}
	}

	private Table parse(String query, LogTableRegistry mockTableRegistry) {
		AccountService mockAccount = mock(AccountService.class);
		LogStorage mockStorage = mock(LogStorage.class);
		LogParserFactoryRegistry mockParserFactoryRegistry = mock(LogParserFactoryRegistry.class);
		LogParserRegistry mockParserRegistry = mock(LogParserRegistry.class);

		when(mockStorage.getStatus()).thenReturn(LogStorageStatus.Open);
		when(mockParserFactoryRegistry.get(null)).thenReturn(null);
		when(mockAccount.checkPermission(null, "iis", Permission.READ)).thenReturn(true);
		when(mockAccount.checkPermission(null, "xtm", Permission.READ)).thenReturn(true);

		TableParser parser = new TableParser(mockAccount, mockStorage, mockTableRegistry, mockParserFactoryRegistry, mockParserRegistry);
		parser.setQueryParserService(queryParserService);
		Table table = (Table) parser.parse(new QueryContext(null), query);
		return table;
	}

	private Table parse(String query) {
		LogTableRegistry mockTableRegistry = mock(LogTableRegistry.class);
		when(mockTableRegistry.exists("iis")).thenReturn(true);
		when(mockTableRegistry.exists("xtm")).thenReturn(true);
		when(mockTableRegistry.getTableNames()).thenReturn(Arrays.asList("iis", "xtm"));

		when(mockTableRegistry.getTableSchema("iis", true)).thenReturn(mockSchema("iis", null));
		when(mockTableRegistry.getTableSchema("xtm", true)).thenReturn(mockSchema("xtm", "trusguard"));

		return parse(query, mockTableRegistry);
	}
}
