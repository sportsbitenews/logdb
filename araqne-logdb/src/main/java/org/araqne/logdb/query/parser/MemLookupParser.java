package org.araqne.logdb.query.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.araqne.logdb.AbstractQueryCommandParser;
import org.araqne.logdb.LookupHandler;
import org.araqne.logdb.LookupHandlerRegistry;
import org.araqne.logdb.MemLookupHandler;
import org.araqne.logdb.QueryCommand;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.command.MemLookup;

public class MemLookupParser extends AbstractQueryCommandParser {

	private LookupHandlerRegistry memLookupRegistry;

	public enum Op {
		LIST, BUILD, DROP;

		public static Op parse(String s) {
			for (Op op : values())
				if (op.name().toLowerCase().equals(s))
					return op;
			throw new QueryParseException("unsupported-memlookup-op", -1);
		}
	}

	public MemLookupParser(LookupHandlerRegistry memLookupRegistry) {
		this.memLookupRegistry = memLookupRegistry;
	}

	@Override
	public String getCommandName() {
		return "memlookup";
	}

	@Override
	public QueryCommand parse(QueryContext context, String commandString) {
		ParseResult r = QueryTokenizer.parseOptions(context, commandString, getCommandName().length(),
				Arrays.asList("op", "name", "key"), getFunctionRegistry());

		@SuppressWarnings("unchecked")
		Map<String, String> opts = (Map<String, String>) r.value;

		Op op = Op.LIST;
		if (opts.get("op") != null) {
			op = Op.parse(opts.get("op"));
		}

		String name = opts.get("name");
		String key = opts.get("key");

		if (op == Op.BUILD) {
			if (name == null)
				throw new QueryParseException("missing-memlookup-name", -1);

			if (key == null)
				throw new QueryParseException("missing-memlookup-key", -1);
		} else if (op == Op.DROP && name == null) {
			throw new QueryParseException("missing-memlookup-name", -1);
		} else if (op == Op.LIST && name != null) {
			LookupHandler handler = memLookupRegistry.getLookupHandler(name);
			if (handler == null || !(handler instanceof MemLookupHandler))
				throw new QueryParseException("invalid-memlookup-name", -1);
		}

		List<String> fields = new ArrayList<String>();
		StringTokenizer tok = new StringTokenizer(commandString.substring(r.next), ",");
		while (tok.hasMoreTokens())
			fields.add(tok.nextToken().trim());

		return new MemLookup(memLookupRegistry, op, name, key, fields);
	}
}
