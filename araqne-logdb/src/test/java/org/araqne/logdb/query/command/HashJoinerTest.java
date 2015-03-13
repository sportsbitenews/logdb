package org.araqne.logdb.query.command;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.query.command.Join.JoinType;
import org.araqne.logdb.query.command.Sort.SortField;
import org.araqne.logdb.query.parser.JsonParser;
import org.araqne.storage.api.RCDirectBufferManager;
import org.junit.Test;

public class HashJoinerTest {
	/*
	@Test
	public void test1() {
		String json = "json \"["
				+ "{'_id': 1, 'field': 'C1'}, "
				+ "{'_id': 2, 'field': 'C2'}, "
				+ "{'_id': 1, 'field': 'C3'}, "
				+ "{'_id': 3, 'field': 'C4'}, "
				+ "{'_id': 1, 'field': 'C5'}]\"";

		JoinType joinType = JoinType.Inner;
		SortField idField = new SortField("_id");
		SortField[] sortFields = { idField };
		HashJoiner joiner = creaetJoiner(joinType, sortFields, json);
		
		Row row1 = createRow(1, "C5");
		assertTrue(joiner.probe(row1).equals(row1.map()));
		
		Row row2 = createRow(2, "C2");
		assertTrue(joiner.probe(row2).equals(row2.map()));
		
		Row row3 = createRow(3, "C4");
		assertTrue(joiner.probe(row3).equals(row3.map()));
		
		Row row4 = createRow(3, "C2");
		assertFalse(joiner.probe(row3).equals(row4.map()));
		
		Row row5 = createRow(11, "C2");
		assertTrue(joiner.probe(row5) == null);
	}
/*	
	private static HashJoiner creaetJoiner(JoinType joinType, SortField[] sortFields, String json) {
		RCDirectBufferManager rcDirectBufferManager = RCDirectBufferManager.getTestManager();
		HashJoiner joiner = new HashJoiner(joinType, sortFields, rcDirectBufferManager);
		RowBatch rowBatch = parseJson(json);
		Iterator<Map<String, Object>> it = rowBatchToIterator(rowBatch);
		
		joiner.build(it);
		return joiner;
	}
	
	private Row createRow(int _id, String field) {
		Map<String, Object> log = new HashMap<String, Object>();
		log.put("_id", _id);
		log.put("field", field);
		Row row = new Row(log);
		
		return row;
	}
	
	private static Iterator<Map<String, Object>> rowBatchToIterator(RowBatch rowBatch) {
		ArrayList<Map<String, Object>> result = new ArrayList<Map<String, Object>>(rowBatch.rows.length);
		for(Row row : rowBatch.rows) {
			result.add(row.map());
		}
		
		return result.iterator();
	}
	
	private static RowBatch parseJson(String json) {
		JsonParser parser = new JsonParser();
		Json rJson = (Json) parser.parse(null, json);
		
		RowBatch rowBatch = new RowBatch();
		rowBatch.rows = rJson.getLogs().toArray(new Row[0]);
		
		return rowBatch;
	}
	*/
}
