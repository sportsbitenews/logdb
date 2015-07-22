package org.araqne.logdb.query.command;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.DriverQueryCommand;
import org.araqne.logdb.LookupHandler;
import org.araqne.logdb.LookupHandlerRegistry;
import org.araqne.logdb.MemLookupHandler;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.QueryStopReason;
import org.araqne.logdb.Row;
import org.araqne.logdb.query.parser.MemLookupParser.Op;

public class MemLookup extends DriverQueryCommand {

	private LookupHandlerRegistry lookupRegistry;
	private Op op;
	private String name;
	private String keyField;
	private List<String> fields;

	private Map<String, Map<String, Object>> mappings = new HashMap<String, Map<String, Object>>();

	public MemLookup(LookupHandlerRegistry lookupRegistry, Op op, String name, String key, List<String> fields) {
		this.lookupRegistry = lookupRegistry;
		this.op = op;
		this.name = name;
		this.keyField = key;
		this.fields = fields;
	}

	@Override
	public String getName() {
		return "memlookup";
	}

	@Override
	public boolean isDriver() {
		return op == Op.LIST;
	}

	@Override
	public void run() {
		if (op != Op.LIST)
			return;

		if (name == null) {

			for (String name : lookupRegistry.getLookupHandlerNames()) {
				LookupHandler h = lookupRegistry.getLookupHandler(name);
				if (!(h instanceof MemLookupHandler))
					continue;

				MemLookupHandler handler = (MemLookupHandler) h;

				Map<String, Object> row = new HashMap<String, Object>();
				row.put("name", name);
				row.put("key", handler.getKeyField());
				row.put("size", handler.getMappings().size());
				pushPipe(new Row(row));
			}
		} else {
			LookupHandler h = lookupRegistry.getLookupHandler(name);
			if (h == null || !(h instanceof MemLookupHandler)){
				//throw new QueryParseException("invalid-memlookup-name", -1);
				Map<String, String> params = new HashMap<String, String >();
				params.put("name", name);
				throw new QueryParseException("22005", -1, -1, params);
			}
			MemLookupHandler handler = (MemLookupHandler) h;
			String keyField = handler.getKeyField();

			Map<String, Map<String, Object>> mappings = handler.getMappings();
			for (String k : mappings.keySet()) {
				// do not corrupt internal map
				Map<String, Object> row = new HashMap<String, Object>(mappings.get(k));
				row.put(keyField, k);
				pushPipe(new Row(row));
			}
		}
	}

	@Override
	public void onPush(Row row) {
		if (op != Op.BUILD) {
			pushPipe(row);
			return;
		}

		Object k = row.get(keyField);
		if (k != null) {
			synchronized (mappings) {
				if (!mappings.containsKey(k.toString()) && mappings.size() < 100000) {
					Map<String, Object> tuple = new HashMap<String, Object>(fields.size());
					for (String field : fields)
						tuple.put(field, row.get(field));

					mappings.put(k.toString(), tuple);
				}
			}
		}

		pushPipe(row);
	}

	@Override
	public void onStart() {
		if (op == Op.DROP) {
			LookupHandler handler = lookupRegistry.getLookupHandler(name);
			if (handler != null && handler instanceof MemLookupHandler)
				lookupRegistry.removeLookupHandler(name);
		}
	}

	@Override
	public void onClose(QueryStopReason reason) {
		if (reason != QueryStopReason.End && reason != QueryStopReason.PartialFetch)
			return;

		if (op == Op.BUILD) {
			LookupHandler handler = lookupRegistry.getLookupHandler(name);
			if (handler == null || handler instanceof MemLookupHandler) {
				lookupRegistry.setLookupHandler(name, new MemLookupHandler(keyField, mappings));
			}
		}
	}
}
