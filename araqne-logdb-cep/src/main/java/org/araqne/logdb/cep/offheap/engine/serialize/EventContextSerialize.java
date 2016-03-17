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
	// public static String delimiter = "-^-";
	private FastEncodingRule enc = new FastEncodingRule();
	private EventKeySerialize keySerializer = new EventKeySerialize();

	// @Override
	// public byte[] serialize(EventContext value) {
	// Map<String, Object> map = marshal(value);
	// ByteBuffer bb = ByteBuffer.allocate(EncodingRule.lengthOf(map));
	// EncodingRule.encode(bb, map);
	// return bb.array();
	// }

	@Override
	public ByteBuffer serialize(EventContext value) {
		Object[] array = marshal(value);
		return enc.encode(array);
	}

	@Override
	public EventContext deserialize(ByteBuffer bb) {
		return parse(EncodingRule.decodeArray(bb));
	}

	// @SuppressWarnings("unchecked")
	// @Override
	// public EventContext deserialize(byte[] in, int offset, int length) {
	// return parse((Map<String, Object>)
	// EncodingRule.decode(ByteBuffer.wrap(in)));
	// }

	@Override
	public int size() {
		return -1;
	}

	public Object[] marshal(EventContext ctx) {
		Object[] array = new Object[8];
		array[0] = keySerializer.serialize(ctx.getKey()).array();
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
		EventKey key = keySerializer.deserialize(ByteBuffer.wrap((byte[])array[0]));
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

	// public Map<String, Object> marshal(EventContext ctx) {
	// HashMap<String, Object> m = new HashMap<String, Object>();
	// m.put("key", marshal(ctx.getKey()));
	// m.put("created", ctx.getCreated());
	// m.put("expireTime", ctx.getExpireTime());
	// m.put("timeoutTime", ctx.getTimeoutTime());
	// m.put("maxRows", ctx.getMaxRows());
	// m.put("rows", format(ctx.getRows()));
	// m.put("variables", ctx.getVariables());
	// m.put("count", ctx.getCounter().get());
	// return m;
	// }

	// @SuppressWarnings("unchecked")
	// public EventContext parse(Map<String, Object> m) {
	// EventKey key = parse((String) m.get("key"));
	// Long created = (Long) m.get("created");
	// Long expireTime = (Long) m.get("expireTime");
	// Long timeoutTime = (Long) m.get("timeoutTime");
	// Integer maxRows = (Integer) m.get("maxRows");
	//
	// List<Row> rows = parseRows((Object[]) m.get("rows"));
	// HashMap<String, Object> variables = (HashMap<String, Object>)
	// m.get("variables");
	// Integer count = (Integer) m.get("count");
	// EventContext cxt = new EventContext(key, created, expireTime,
	// timeoutTime, maxRows);// ,
	// // host);
	// for (Row row : rows)
	// cxt.addRow(row);
	//
	// if (count != null)
	// cxt.getCounter().addAndGet(count);
	//
	// if (variables != null)
	// for (String s : variables.keySet())
	// cxt.setVariable(s, variables.get(s));
	//
	// return cxt;
	// }

//	public String marshal(EventKey key) {
//		StringBuffer sb = new StringBuffer();
//		sb.append(key.getTopic());
//		sb.append(delimiter);
//		sb.append(key.getKey());
//		sb.append(delimiter);
//		if (key.getHost() != null)
//			sb.append(key.getHost());
//		return sb.toString();
//	}
//
//	public EventKey parse(String line) {
//		String[] parsed = new String[3];
//
//		int i = 0;
//		int last = 0;
//		while (true) {
//			int p = line.indexOf(delimiter, last);
//			String token = null;
//			if (p >= 0)
//				token = line.substring(last, p);
//			else
//				token = line.substring(last);
//
//			if (token.isEmpty())
//				token = null;
//
//			parsed[i] = token;
//			if (p < 0)
//				break;
//
//			last = p + delimiter.length();
//			i++;
//		}
//		EventKey evtkey = new EventKey(parsed[0], parsed[1], parsed[2]);
//		return evtkey;
//	}

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
