package org.araqne.logdb.query.command;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.araqne.codec.EncodingRule;
import org.araqne.logdb.Row;
import org.araqne.logdb.query.command.Join.JoinKeys;
import org.araqne.logdb.query.command.Join.JoinType;
import org.araqne.logdb.query.command.Sort.SortField;

public class HashJoiner {
	// TODO: fix the number;
	private static final int CAPACITY = 1024 * 1;
	private JoinType joinType;
	private SortField[] sortFields;

	private Map<JoinKeys, Integer> map;
	private ByteBuffer logByteBuffer;
	private boolean buildComplete;

	public HashJoiner(JoinType joinType, SortField[] sortFields) {
		this.joinType = joinType;
		this.sortFields = sortFields;

		this.map = new HashMap<JoinKeys, Integer>();
		//TODO:
		//should be changed to allocate direct.
		logByteBuffer = ByteBuffer.allocate(CAPACITY);
	}

	public void build(Iterator<Map<String, Object>> it) {
		while (it.hasNext()) {
			Map<String, Object> log = it.next();

			JoinKeys key = extractKey(log);
			int writePosition = writeLog(log);
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
			logByteBuffer.position(writePosition);
			return (Map<String, Object>) EncodingRule.decode(logByteBuffer);
		}
	}

	private JoinKeys extractKey(Map<String, Object> log) {
		Object[] keys = new Object[sortFields.length];
		int i = 0;
		for (SortField sortField : sortFields) {
			keys[i++] = log.get(sortField.getName());
		}

		return new JoinKeys(keys);
	}

	private int writeLog(Map<String, Object> log) {
		int result = logByteBuffer.position();
		EncodingRule.encode(logByteBuffer, log);

		return result;
	}
}
