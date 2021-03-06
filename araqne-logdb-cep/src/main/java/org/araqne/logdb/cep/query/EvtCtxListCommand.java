package org.araqne.logdb.cep.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.araqne.logdb.DriverQueryCommand;
import org.araqne.logdb.FieldOrdering;
import org.araqne.logdb.Row;
import org.araqne.logdb.cep.EventContext;
import org.araqne.logdb.cep.EventContextService;
import org.araqne.logdb.cep.EventContextStorage;
import org.araqne.logdb.cep.EventKey;

public class EvtCtxListCommand extends DriverQueryCommand implements FieldOrdering {

	private EventContextService eventContextService;
	private String topicFilter;

	public EvtCtxListCommand(EventContextService eventContextService, String topicFilter) {
		this.eventContextService = eventContextService;
		this.topicFilter = topicFilter;
	}

	@Override
	public String getName() {
		return "evtctxlst";
	}

	@Override
	public List<String> getFieldOrder() {
		return Arrays.asList("topic", "key", "host", "counter", "created", "expire_at", "timeout_at", "maxrows", "vars");
	}

	@Override
	public void run() {
		EventContextStorage storage = eventContextService.getDefaultStorage();
		Iterator<EventKey> eventKeyItr = storage.getContextKeys(topicFilter);

		Set<EventKey> keys = new HashSet<EventKey>();
		int i = 0;
		while (eventKeyItr.hasNext()) {
			EventKey key = eventKeyItr.next();
			keys.add(key);

			if (++i >= 5000) {
				pushEventContexts(storage.getContexts(keys));
				keys.clear();
				i = 0;
			}
		}

		pushEventContexts(storage.getContexts(keys));
	}

	private void pushEventContexts(List<EventContext> events) {

		for (EventContext ctx : events) {
			if (ctx == null)
				continue;

			EventKey key = ctx.getKey();
			
			String topic = key.getTopic();
			if (topicFilter != null && !topic.equals(topicFilter))
				continue;

			Row row = new Row();
			row.put("topic", topic);
			row.put("key", key.getKey());
			row.put("host", key.getHost());
			row.put("counter", ctx.getCounter().get());
			row.put("created", new Date(ctx.getCreated()));
			row.put("expire_at", ctx.getExpireTime() == 0 ? null : new Date(ctx.getExpireTime()));
			row.put("timeout_at", ctx.getTimeoutTime() == 0 ? null : new Date(ctx.getTimeoutTime()));
			row.put("maxrows", ctx.getMaxRows());
			row.put("vars", ctx.getVariables());

			// following query command can corrupt data in cep context
			List<Object> rows = new ArrayList<Object>();
			for (Row r : ctx.getRows())
				rows.add(Row.clone(r.map()));

			row.put("rows", rows);
			pushPipe(row);
		}
	}

	@Override
	public String toString() {
		String opt = "";
		if (topicFilter != null)
			opt = " topic=" + topicFilter;

		return "evtctxlist" + opt;
	}
}
