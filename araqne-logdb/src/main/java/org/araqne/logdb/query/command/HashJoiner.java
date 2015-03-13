package org.araqne.logdb.query.command;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.araqne.codec.EncodingRule;
import org.araqne.logdb.ByteBufferResult.ByteBufferResultSet;
import org.araqne.logdb.Row;
import org.araqne.logdb.query.command.Join.JoinKeys;
import org.araqne.logdb.query.command.Join.JoinType;
import org.araqne.logdb.query.command.Sort.SortField;

public class HashJoiner {
	// TODO: fix the number;
	private static final int CAPACITY = 1024 * 1024 * 700;
	private JoinType joinType;
	private SortField[] sortFields;

	private Map<JoinKeys, Integer> map;
	private ByteBuffer byteBuffer;
	private boolean buildComplete;

	public HashJoiner(JoinType joinType, SortField[] sortFields) {
		this.joinType = joinType;
		this.sortFields = sortFields;

		this.map = new HashMap<JoinKeys, Integer>(CAPACITY / 1024);
	}

	private int count;
	public void build(ByteBufferResultSet it) {
		this.byteBuffer = it.getByteBuffer();
		while (it.hasNext()) {
			int writePosition = it.getPosition();
			Map<String, Object> log = it.next();
			JoinKeys key = extractKey(log);
			map.put(key, writePosition);
		}

		buildComplete = true;
	}

	@SuppressWarnings("unchecked")
	public Map<String, Object> probe(Row row) {
		// the probe phase should be called after build phase.
		// the probe phase move position of ByteBuffer.
		// it could affect to build phase.Becuase build(write to ByteBuffer) depends on position.
		// so throw exception when the build phase is not completed.
		if (!buildComplete)
			throw new IllegalStateException("build phase should be completed");

		Map<String, Object> log = row.map();

		JoinKeys key = extractKey(log);
		Integer writePosition = map.get(key);
		if (writePosition == null)
			return null;
		else {
			byteBuffer.position(writePosition);
			return (Map<String, Object>) EncodingRule.decode(byteBuffer);
		}
	}
	
	public void close() {
	}
	
	public void cancel() {
		this.close();
	}

	private JoinKeys extractKey(Map<String, Object> log) {
		Object[] keys = new Object[sortFields.length];
		int i = 0;
		for (SortField sortField : sortFields) {
			keys[i++] = log.get(sortField.getName());
		}

		return new JoinKeys(keys);
	}
}
