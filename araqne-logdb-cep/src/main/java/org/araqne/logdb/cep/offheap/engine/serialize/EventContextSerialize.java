package org.araqne.logdb.cep.offheap.engine.serialize;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.codec.EncodingRule;
import org.araqne.codec.FastEncodingRule;
import org.araqne.logdb.Row;
import org.araqne.logdb.cep.EventContext;
import org.araqne.logdb.cep.EventKey;

public class EventContextSerialize implements Serialize<EventContext> {
	private FastEncodingRule enc = new FastEncodingRule();
	private EventKeySerialize keySerializer = new EventKeySerialize();

	@Override
	public byte[] serialize(EventContext value) {
		Object[] array = marshal(value);
		return enc.encode(array).array();
	}

	@Override
	public EventContext deserialize(byte[] in) {
		return parse(EncodingRule.decodeArray(ByteBuffer.wrap(in)));
	}

	@Override
	public int size() {
		return -1;
	}

	public Object[] marshal(EventContext ctx) {
		Object[] array = new Object[8];
		array[0] = keySerializer.serialize(ctx.getKey());
		array[1] = ctx.getCreated();
		array[2] = ctx.getExpireTime();
		array[3] = ctx.getTimeoutTime();
		array[4] = ctx.getMaxRows();
		array[5] = format(ctx.getRows());
		array[6] = ctx.getVariables();
		array[7] = ctx.getCounter().get();
		return array;
	}

	@SuppressWarnings("unchecked")
	public EventContext parse(Object[] array) {
		EventKey key = keySerializer.deserialize((byte[]) array[0]);
		Long created = (Long) array[1];
		Long expireTime = (Long) array[2];
		Long timeoutTime = (Long) array[3];
		Integer maxRows = (Integer) array[4];
		List<Row> rows = parseRows((Object[]) array[5]);
		HashMap<String, Object> variables = (HashMap<String, Object>) array[6];
		Integer count = (Integer) array[7];

		EventContext cxt = new EventContext(key, created, expireTime, timeoutTime, maxRows);
		for (Row row : rows)
			cxt.addRow(row);

		if (count != null)
			cxt.getCounter().addAndGet(count);

		if (variables != null)
			for (String s : variables.keySet())
				cxt.setVariable(s, variables.get(s));

		return cxt;
	}

	private List<Map<String, Object>> format(List<Row> rows) {
		List<Map<String, Object>> rowMaps = new ArrayList<Map<String, Object>>();
		for (Row row : rows) {
			rowMaps.add(row.map());
		}
		return rowMaps;
	}

	@SuppressWarnings("unchecked")
	private List<Row> parseRows(Object[] rowMaps) {
		List<Row> rows = Collections.synchronizedList(new ArrayList<Row>());
		for (Object rowMap : rowMaps) {
			rows.add(new Row((Map<String, Object>) rowMap));
		}
		return rows;
	}
	
}
