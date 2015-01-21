package org.araqne.logdb.query.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.Row;
import org.araqne.logdb.RowBatch;
import org.araqne.logdb.RowPipe;
import org.araqne.logdb.query.command.Join.JoinType;
import org.araqne.logdb.query.command.SortMergeJoiner;
import org.araqne.logdb.query.parser.JsonParser;
import org.junit.Test;
import org.araqne.logdb.query.command.Sort.SortField;

import static org.junit.Assert.*;

public class SortMergeJoinerTest {

	private static List<RowBatch> jsonToRowBatch(String json) {
		JsonParser parser = new JsonParser();
		Json rJson = (Json) parser.parse(null, json);

		RowBatch rowBatch = new RowBatch();
		rowBatch.rows = rJson.getLogs().toArray(new Row[0]);
		rowBatch.size = rowBatch.rows.length;

		List<RowBatch> result = new ArrayList<RowBatch>();
		result.add(rowBatch);
		return result;
	}

	private static List<Map<String, Object>> jsonToMaps(String json) {
		JsonParser parser = new JsonParser();
		Json sJson = (Json) parser.parse(null, json);

		List<Map<String, Object>> maps = new ArrayList<Map<String, Object>>();
		for (Row row : sJson.getLogs()) {
			maps.add(row.map());
		}

		return maps;
	}

	private static void runJoin(String rTableJson, String sTableJson, String expectedResultJson, SortField[] sortFields) throws IOException {
		List<RowBatch> unsortedR = jsonToRowBatch(rTableJson);
		List<Map<String, Object>> unsortedS = jsonToMaps(sTableJson);

		RowPipeForTest output = new RowPipeForTest();

		SortMergeJoiner sortMergeJoiner = new SortMergeJoiner(JoinType.Inner, sortFields);
		sortMergeJoiner.setOutput(output);
		sortMergeJoiner.setS(unsortedS.iterator());
		sortMergeJoiner.setR(unsortedR);

		sortMergeJoiner.merge();

		List<Map<String, Object>> expectedResult = jsonToMaps(expectedResultJson);
		Iterator<Map<String, Object>> it = expectedResult.iterator();
		for (Row row : output.getRows()) {
			Map<String, Object> expected = it.next();
			assertTrue(expected.equals(row.map()));
		}

		assertEquals(expectedResult.size(), output.getRows().size());
	}
	
	private static void runJoin(String rTableJson, String sTableJson, String expectedResultJson) throws IOException {
		SortField[] sortFields = { new SortField("_id") };
		runJoin(rTableJson, sTableJson, expectedResultJson, sortFields);
	}
	
	private static void runJoin2Times(String rTableJson, String sTableJson, String expectedResultJson1, String expectedResultJson2) throws IOException {
		List<RowBatch> unsortedR = jsonToRowBatch(rTableJson);
		List<Map<String, Object>> unsortedS = jsonToMaps(sTableJson);

		SortField[] sortFields = { new SortField("_id") };
		RowPipeForTest output = new RowPipeForTest();

		SortMergeJoiner sortMergeJoiner = new SortMergeJoiner(JoinType.Inner, sortFields);
		sortMergeJoiner.setOutput(output);
		sortMergeJoiner.setS(unsortedS.iterator());
		sortMergeJoiner.setR(unsortedR);

		//merge and test
		sortMergeJoiner.merge();

		List<Map<String, Object>> expectedResult1 = jsonToMaps(expectedResultJson1);
		Iterator<Map<String, Object>> it1 = expectedResult1.iterator();
		for (Row row : output.getRows()) {
			Map<String, Object> expected = it1.next();
			assertTrue(expected.equals(row.map()));
		}

		assertEquals(expectedResult1.size(), output.getRows().size());
	
		
		//merge and test
		sortMergeJoiner.setR(unsortedR);
		sortMergeJoiner.merge();
		
		List<Map<String, Object>> expectedResult2 = jsonToMaps(expectedResultJson1);
		expectedResult2.addAll(expectedResult1);
		Iterator<Map<String, Object>> it2 = expectedResult2.iterator();
		for (Row row : output.getRows()) {
			Map<String, Object> expected = it2.next();
			assertTrue(expected.equals(row.map()));
		}

		assertEquals(expectedResult2.size(), output.getRows().size());
	}


	@Test
	public void sortMergeJoinerInnerJoinTest1() throws IOException {
		String rTableJson = "json \"["
				+ "{'_id': 0, 'fieldA': 'A1'}, "
				+ "{'_id': 1, 'fieldA': 'A2'}, "
				+ "{'_id': 2, 'fieldA': 'A3'},"
				+ "{'_id': 1, 'fieldA': 'A4'}"
				+ "]\"";

		String sTableJson = "json \"["
				+ "{'_id': 1, 'fieldC': 'C1'}, "
				+ "{'_id': 2, 'fieldC': 'C2'}, "
				+ "{'_id': 1, 'fieldC': 'C3'}, "
				+ "{'_id': 3, 'fieldC': 'C4'}, "
				+ "{'_id': 1, 'fieldC': 'C5'}]\"";

		String expectedResult = "json \"["
				+ "{'fieldA' = 'A2', _id=1, 'fieldC' = 'C1'}, "
				+ "{'fieldA' = 'A2', _id=1, 'fieldC' = 'C3'}, "
				+ "{'fieldA' = 'A2', _id=1, 'fieldC' = 'C5'}, "
				+ "{'fieldA' = 'A4', _id=1, 'fieldC' = 'C1'}, "
				+ "{'fieldA' = 'A4', _id=1, 'fieldC' = 'C3'}, "
				+ "{'fieldA' = 'A4', _id=1, 'fieldC' = 'C5'}, "
				+ "{'fieldA' = 'A3', _id=2, 'fieldC' = 'C2'}, "
				+ "]\"";

		runJoin(rTableJson, sTableJson, expectedResult);
	}

	@Test
	public void sortMergeJoinerInnerJoinTest2() throws IOException {
		String rTableJson = "json \"["
				+ "{'_id': 0, 'fieldA': 'A1'}, "
				+ "{'_id': 1, 'fieldA': 'A2'}, "
				+ "{'_id': 2, 'fieldA': 'A3'},"
				+ "{'_id': 1, 'fieldA': 'A4'}"
				+ "]\"";

		String sTableJson = "json \"["
				+ "{'_id': 1, 'fieldC': 'C1'}, "
				+ "{'_id': 1, 'fieldC': 'C3'}, "
				+ "{'_id': 1, 'fieldC': 'C5'}]\"";

		String expectedResult = "json \"["
				+ "{'fieldA' = 'A2', _id=1, 'fieldC' = 'C1'}, "
				+ "{'fieldA' = 'A2', _id=1, 'fieldC' = 'C3'}, "
				+ "{'fieldA' = 'A2', _id=1, 'fieldC' = 'C5'}, "
				+ "{'fieldA' = 'A4', _id=1, 'fieldC' = 'C1'}, "
				+ "{'fieldA' = 'A4', _id=1, 'fieldC' = 'C3'}, "
				+ "{'fieldA' = 'A4', _id=1, 'fieldC' = 'C5'}, "
				+ "]\"";

		runJoin(rTableJson, sTableJson, expectedResult);
	}

	@Test
	public void sortMergeJoinerInnerJoinTest3() throws IOException {
		String rTableJson = "json \"["
				+ "]\"";

		String sTableJson = "json \"["
				+ "{'_id': 1, 'fieldC': 'C1'}, "
				+ "{'_id': 1, 'fieldC': 'C3'}, "
				+ "{'_id': 1, 'fieldC': 'C5'}]\"";

		String expectedResult = "json \"["
				+ "]\"";

		runJoin(rTableJson, sTableJson, expectedResult);
	}

	@Test
	public void sortMergeJoinerInnerJoinTest4() throws IOException {
		String rTableJson = "json \"["
				+ "{'_id': 0, 'fieldA': 'A1'}, "
				+ "{'_id': 1, 'fieldA': 'A2'}, "
				+ "{'_id': 2, 'fieldA': 'A3'},"
				+ "{'_id': 1, 'fieldA': 'A4'}"
				+ "]\"";

		String sTableJson = "json \"["
				+ "]\"";

		String expectedResult = "json \"["
				+ "]\"";

		runJoin(rTableJson, sTableJson, expectedResult);
	}

	@Test
	public void sortMergeJoinerInnerJoinTest5() throws IOException {
		String rTableJson = "json \"["
				+ "{'_id': 0, 'fieldA': 'A1'}, "
				+ "{'_id': 1, 'fieldA': 'A2'}, "
				+ "{'_id': 2, 'fieldA': 'A3'}, "
				+ "{'_id': 1, 'fieldA': 'A4'}, "
				+ "{'_id': 3, 'fieldA': 'A5'}, "
				+ "{'_id': 3, 'fieldA': 'A6'}, "
				+ "{'_id': 4, 'fieldA': 'A7'}, "
				+ "{'_id': 4, 'fieldA': 'A8'}, "
				+ "{'_id': 5, 'fieldA': 'A9'}, "
				+ "{'_id': 5, 'fieldA': 'A10'}, "
				+ "{'_id': 5, 'fieldA': 'A11'}"
				+ "]\"";

		String sTableJson = "json \"["
				+ "{'_id': 1, 'fieldC': 'C1'}, "
				+ "{'_id': 2, 'fieldC': 'C2'}, "
				+ "{'_id': 1, 'fieldC': 'C3'}, "
				+ "{'_id': 3, 'fieldC': 'C4'}, "
				+ "{'_id': 1, 'fieldC': 'C5'}]\"";

		String expectedResult = "json \"["
				+ "{'fieldA' = 'A2', _id=1, 'fieldC' = 'C1'}, "
				+ "{'fieldA' = 'A2', _id=1, 'fieldC' = 'C3'}, "
				+ "{'fieldA' = 'A2', _id=1, 'fieldC' = 'C5'}, "
				+ "{'fieldA' = 'A4', _id=1, 'fieldC' = 'C1'}, "
				+ "{'fieldA' = 'A4', _id=1, 'fieldC' = 'C3'}, "
				+ "{'fieldA' = 'A4', _id=1, 'fieldC' = 'C5'}, "
				+ "{'fieldA' = 'A3', _id=2, 'fieldC' = 'C2'}, "
				+ "{'fieldA' = 'A5', _id=3, 'fieldC' = 'C4'}, "
				+ "{'fieldA' = 'A6', _id=3, 'fieldC' = 'C4'}, "
				+ "]\"";

		runJoin(rTableJson, sTableJson, expectedResult);
	}
	
	@Test
	public void sortMergeJoinerInnerJoinTest6() throws IOException {
		String rTableJson = "json \"["
				+ "{'_id': 0, 'fieldA': 'A1'}, "
				+ "{'_id': 1, 'fieldA': 'A2'}, "
				+ "{'_id': 2, 'fieldA': 'A3'}, "
				+ "{'_id': 1, 'fieldA': 'A4'}, "
				+ "{'_id': 3, 'fieldA': 'A5'}, "
				+ "{'_id': 3, 'fieldA': 'A6'}, "
				+ "{'_id': 4, 'fieldA': 'A7'}, "
				+ "{'_id': 4, 'fieldA': 'A8'}, "
				+ "{'_id': 5, 'fieldA': 'A9'}, "
				+ "{'_id': 5, 'fieldA': 'A10'}, "
				+ "{'_id': 5, 'fieldA': 'A11'}"
				+ "]\"";

		String sTableJson = "json \"["
				+ "{'_id': 1, 'fieldC': 'C1'}, "
				+ "{'_id': 2, 'fieldC': 'C2'}, "
				+ "{'_id': 1, 'fieldC': 'C3'}, "
				+ "{'_id': 3, 'fieldC': 'C4'}, "
				+ "{'_id': 1, 'fieldC': 'C5'}]\"";

		String expectedResult = "json \"["
				+ "{'fieldA' = 'A2', _id=1, 'fieldC' = 'C1'}, "
				+ "{'fieldA' = 'A2', _id=1, 'fieldC' = 'C3'}, "
				+ "{'fieldA' = 'A2', _id=1, 'fieldC' = 'C5'}, "
				+ "{'fieldA' = 'A4', _id=1, 'fieldC' = 'C1'}, "
				+ "{'fieldA' = 'A4', _id=1, 'fieldC' = 'C3'}, "
				+ "{'fieldA' = 'A4', _id=1, 'fieldC' = 'C5'}, "
				+ "{'fieldA' = 'A3', _id=2, 'fieldC' = 'C2'}, "
				+ "{'fieldA' = 'A5', _id=3, 'fieldC' = 'C4'}, "
				+ "{'fieldA' = 'A6', _id=3, 'fieldC' = 'C4'}, "
				+ "]\"";
		
		
		

		//runJoin2Times(rTableJson, sTableJson, expectedResult, expectedResult);
	}
	
	@Test
	public void sortMergeJoinerInnerJoinTest7() throws IOException {
		String rTableJson = "json \"["
				+ "{'temp' : 0, '_id': 0, 'fieldA': 'A1'}, "
				+ "{'temp' : 0, '_id': 1, 'fieldA': 'A2'}, "
				+ "{'temp' : 0, '_id': 2, 'fieldA': 'A3'}, "
				+ "{'temp' : 0, '_id': 1, 'fieldA': 'A4'}, "
				+ "]\"";

		String sTableJson = "json \"["
				+ "{'temp' : 0, '_id': 1, 'fieldC': 'C1'}, "
				+ "{'temp' : 0, '_id': 2, 'fieldC': 'C2'}, "
				+ "{'temp' : 0, '_id': 1, 'fieldC': 'C3'}, "
				+ "{'temp' : 0, '_id': 3, 'fieldC': 'C4'}, "
				+ "{'temp' : 0, '_id': 1, 'fieldC': 'C5'}]\"";

		String expectedResult = "json \"["
				+ "{'temp' : 0, 'fieldA' = 'A2', _id=1, 'fieldC' = 'C1'}, "
				+ "{'temp' : 0, 'fieldA' = 'A2', _id=1, 'fieldC' = 'C3'}, "
				+ "{'temp' : 0, 'fieldA' = 'A2', _id=1, 'fieldC' = 'C5'}, "
				+ "{'temp' : 0, 'fieldA' = 'A4', _id=1, 'fieldC' = 'C1'}, "
				+ "{'temp' : 0, 'fieldA' = 'A4', _id=1, 'fieldC' = 'C3'}, "
				+ "{'temp' : 0, 'fieldA' = 'A4', _id=1, 'fieldC' = 'C5'}, "
				+ "{'temp' : 0, 'fieldA' = 'A3', _id=2, 'fieldC' = 'C2'}, "
				+ "]\"";

		SortField[] sortFields = { new SortField("_id"), new SortField("temp") };
		runJoin(rTableJson, sTableJson, expectedResult, sortFields);
	}
	
	@Test
	public void sortMergeJoinerInnerJoinTest8() throws IOException {
		String rTableJson = "json \"["
				+ "{'_id'=1, 'line'='2007-10-13 06:20:46 W3SVC1 123.223.21.233 GET /solution/1.982/asp/strawlv01982_msg.asp t=1&m=001921F08323 80 - 125.240.40.73 UtilMind+HTTPGet 404 0 3'}," 
				+ "{'_id'=2, 'line'='2007-10-13 06:20:46 W3SVC1 123.223.21.233 GET /solution/1.982/asp/strawlv01982_msg.asp t=1&m=00022AB01065 80 - 121.265.247.218 UtilMind+HTTPGet 404 0 3'}," 
				+ "{'_id'=3, 'line'='2007-10-13 06:20:46 W3SVC1 123.223.21.233 GET /solution/1.982/asp/strawlv01982_msg.asp t=1&m=000FB0743301 80 - 123.204.294.39 UtilMind+HTTPGet 404 0 3'}," 
				+ "{'_id'=4, 'line'='2007-10-13 06:20:46 W3SVC1 123.223.21.233 GET /solution/1.982/asp/strawlv01982_msg.asp t=1&m=0000F07EA104 80 - 121.225.264.29 UtilMind+HTTPGet 404 0 3'}," 
				+ "{'_id'=5, 'line'='2007-10-13 06:20:46 W3SVC1 123.223.21.233 GET /solution/1.982/asp/strawlv01982_msg.asp t=1&m=0007E913F5AD 80 - 103.241.247.26 UtilMind+HTTPGet 404 0 3'}," 
				+ "{'_id'=6, 'line'='2007-10-13 06:20:46 W3SVC1 123.223.21.233 GET /solution/1.982/asp/strawlv01982_msg.asp t=1&m=00155889D370 80 - 121.261.33.20 UtilMind+HTTPGet 404 0 3'}," 
				+ "{'_id'=7, 'line'='2007-10-13 06:20:46 W3SVC1 123.223.21.233 GET /solution/1.982/asp/strawlv01982_msg.asp t=1&m=0019DB807868 80 - 124.53.263.24 UtilMind+HTTPGet 404 0 3'}," 
				+ "{'_id'=8, 'line'='2007-10-13 06:20:46 W3SVC1 123.223.21.233 GET /solution/1.982/asp/strawlv01982_msg.asp t=1&m=0002724BC802 80 - 61.255.240.288 UtilMind+HTTPGet 404 0 3'}," 
				+ "{'_id'=9, 'line'='2007-10-13 06:20:46 W3SVC1 123.223.21.233 GET /solution/1.982/asp/strawlv01982_msg.asp t=1&m=000B6AD52272 80 - 121.0.228.218 UtilMind+HTTPGet 404 0 3'}" 
				+ "]\"";

		String sTableJson = "json \"["
				+ "{'_id'=1, 'line'='2007-10-13 06:20:46 W3SVC1 123.223.21.233 GET /solution/1.982/asp/strawlv01982_msg.asp t=1&m=001921F08323 80 - 125.240.40.73 UtilMind+HTTPGet 404 0 3'}," 
				+ "{'_id'=2, 'line'='2007-10-13 06:20:46 W3SVC1 123.223.21.233 GET /solution/1.982/asp/strawlv01982_msg.asp t=1&m=00022AB01065 80 - 121.265.247.218 UtilMind+HTTPGet 404 0 3'}," 
				+ "{'_id'=3, 'line'='2007-10-13 06:20:46 W3SVC1 123.223.21.233 GET /solution/1.982/asp/strawlv01982_msg.asp t=1&m=000FB0743301 80 - 123.204.294.39 UtilMind+HTTPGet 404 0 3'}," 
				+ "{'_id'=4, 'line'='2007-10-13 06:20:46 W3SVC1 123.223.21.233 GET /solution/1.982/asp/strawlv01982_msg.asp t=1&m=0000F07EA104 80 - 121.225.264.29 UtilMind+HTTPGet 404 0 3'}," 
				+ "{'_id'=5, 'line'='2007-10-13 06:20:46 W3SVC1 123.223.21.233 GET /solution/1.982/asp/strawlv01982_msg.asp t=1&m=0007E913F5AD 80 - 103.241.247.26 UtilMind+HTTPGet 404 0 3'}," 
				+ "{'_id'=6, 'line'='2007-10-13 06:20:46 W3SVC1 123.223.21.233 GET /solution/1.982/asp/strawlv01982_msg.asp t=1&m=00155889D370 80 - 121.261.33.20 UtilMind+HTTPGet 404 0 3'}," 
				+ "{'_id'=7, 'line'='2007-10-13 06:20:46 W3SVC1 123.223.21.233 GET /solution/1.982/asp/strawlv01982_msg.asp t=1&m=0019DB807868 80 - 124.53.263.24 UtilMind+HTTPGet 404 0 3'}," 
				+ "{'_id'=8, 'line'='2007-10-13 06:20:46 W3SVC1 123.223.21.233 GET /solution/1.982/asp/strawlv01982_msg.asp t=1&m=0002724BC802 80 - 61.255.240.288 UtilMind+HTTPGet 404 0 3'}," 
				+ "{'_id'=9, 'line'='2007-10-13 06:20:46 W3SVC1 123.223.21.233 GET /solution/1.982/asp/strawlv01982_msg.asp t=1&m=000B6AD52272 80 - 121.0.228.218 UtilMind+HTTPGet 404 0 3'}" 
				+ "]\"";

		String expectedResult = "json \"["
				+ "{'_id'=3, 'line'='2007-10-13 06:20:46 W3SVC1 123.223.21.233 GET /solution/1.982/asp/strawlv01982_msg.asp t=1&m=000FB0743301 80 - 123.204.294.39 UtilMind+HTTPGet 404 0 3'}," 
				+ "{'_id'=8, 'line'='2007-10-13 06:20:46 W3SVC1 123.223.21.233 GET /solution/1.982/asp/strawlv01982_msg.asp t=1&m=0002724BC802 80 - 61.255.240.288 UtilMind+HTTPGet 404 0 3'}," 
				+ "{'_id'=5, 'line'='2007-10-13 06:20:46 W3SVC1 123.223.21.233 GET /solution/1.982/asp/strawlv01982_msg.asp t=1&m=0007E913F5AD 80 - 103.241.247.26 UtilMind+HTTPGet 404 0 3'}," 
				+ "{'_id'=7, 'line'='2007-10-13 06:20:46 W3SVC1 123.223.21.233 GET /solution/1.982/asp/strawlv01982_msg.asp t=1&m=0019DB807868 80 - 124.53.263.24 UtilMind+HTTPGet 404 0 3'}," 
				+ "{'_id'=4, 'line'='2007-10-13 06:20:46 W3SVC1 123.223.21.233 GET /solution/1.982/asp/strawlv01982_msg.asp t=1&m=0000F07EA104 80 - 121.225.264.29 UtilMind+HTTPGet 404 0 3'}," 
				+ "{'_id'=1, 'line'='2007-10-13 06:20:46 W3SVC1 123.223.21.233 GET /solution/1.982/asp/strawlv01982_msg.asp t=1&m=001921F08323 80 - 125.240.40.73 UtilMind+HTTPGet 404 0 3'}," 
				+ "{'_id'=2, 'line'='2007-10-13 06:20:46 W3SVC1 123.223.21.233 GET /solution/1.982/asp/strawlv01982_msg.asp t=1&m=00022AB01065 80 - 121.265.247.218 UtilMind+HTTPGet 404 0 3'}," 
				+ "{'_id'=9, 'line'='2007-10-13 06:20:46 W3SVC1 123.223.21.233 GET /solution/1.982/asp/strawlv01982_msg.asp t=1&m=000B6AD52272 80 - 121.0.228.218 UtilMind+HTTPGet 404 0 3'}," 
				+ "{'_id'=6, 'line'='2007-10-13 06:20:46 W3SVC1 123.223.21.233 GET /solution/1.982/asp/strawlv01982_msg.asp t=1&m=00155889D370 80 - 121.261.33.20 UtilMind+HTTPGet 404 0 3'}," 
				+ "]\"";

		SortField[] sortFields = { new SortField("_id"), new SortField("line") };
		runJoin(rTableJson, sTableJson, expectedResult, sortFields);
	}


	public static class RowPipeForTest implements RowPipe {
		private List<Row> rows;

		private RowPipeForTest() {
			rows = new ArrayList<Row>();
		}

		@Override
		public boolean isThreadSafe() {
			return false;
		}

		@Override
		public void onRow(Row row) {
			rows.add(row);
		}

		@Override
		public void onRowBatch(RowBatch rowBatch) {
			if (rowBatch.selectedInUse) {
				for (int i = 0; i < rowBatch.size; i++) {
					Row row = rowBatch.rows[rowBatch.selected[i]];
					onRow(row);
				}
			} else {
				for (Row row : rowBatch.rows) {
					onRow(row);
				}
			}

		}

		public List<Row> getRows() {
			return rows;
		}
	}
}
