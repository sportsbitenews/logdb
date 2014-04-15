package org.araqne.logdb.metadata;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.AccountService;
import org.araqne.logdb.Privilege;
import org.araqne.logdb.QueryContext;
import org.araqne.logdb.QueryParseException;
import org.araqne.logdb.query.parser.CommandOptions;
import org.araqne.logdb.query.parser.ParseResult;
import org.araqne.logdb.query.parser.QueryTokenizer;
import org.araqne.logstorage.LogTableRegistry;

public class MetadataQueryStringParser {

	public static TableScanOption getTableNames(QueryContext context, LogTableRegistry tableRegistry,
			AccountService accountService, String token) {
		token = token.trim();

		Date from = null;
		Date to = null;

		ParseResult r = QueryTokenizer.parseOptions(context, token, 0, Arrays.asList("duration", "from", "to", "diskonly"));

		@SuppressWarnings("unchecked")
		Map<String, String> optionTokens = (Map<String, String>) r.value;

		String duration = optionTokens.get("duration");
		if (duration != null) {
			int i;
			for (i = 0; i < duration.length(); i++) {
				char c = duration.charAt(i);
				if (!('0' <= c && c <= '9'))
					break;
			}
			int value = Integer.parseInt(duration.substring(0, i));
			from = QueryTokenizer.getDuration(value, duration.substring(i));
		}

		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
		String fromToken = optionTokens.get("from");
		if (fromToken != null) {
			from = df.parse(fromToken, new ParsePosition(0));
			if (from == null)
				throw new QueryParseException("invalid-from", -1);
		}

		String toToken = optionTokens.get("to");
		if (toToken != null) {
			to = df.parse(toToken, new ParsePosition(0));
			if (to == null)
				throw new QueryParseException("invalid-to", -1);
		}

		if (to != null && from != null && to.before(from))
			throw new QueryParseException("invalid-date-range", -1);

		int next = r.next;
		token = token.substring(next).trim();

		HashSet<String> tableFilter = null;
		String[] argTableNames = null;
		if (!token.isEmpty()) {
			tableFilter = new HashSet<String>();
			argTableNames = token.split(",");

			for (int i = 0; i < argTableNames.length; i++)
				tableFilter.add(argTableNames[i].trim());
		}

		List<String> tableNames = new ArrayList<String>();

		if (context.getSession().isAdmin()) {
			for (String tableName : tableRegistry.getTableNames()) {
				if (tableFilter != null && !tableFilter.contains(tableName))
					continue;

				tableNames.add(tableName);
			}
		} else {
			List<Privilege> privileges = accountService.getPrivileges(context.getSession(), context.getSession().getLoginName());
			for (Privilege p : privileges) {
				if (p.getPermissions().size() > 0 && tableRegistry.exists(p.getTableName())) {
					if (tableFilter != null && !tableFilter.contains(p.getTableName()))
						continue;

					tableNames.add(p.getTableName());
				}
			}
		}

		TableScanOption opt = new TableScanOption();
		opt.setTableNames(tableNames);
		opt.setFrom(from);
		opt.setTo(to);
		opt.setDiskOnly(CommandOptions.parseBoolean((String) optionTokens.get("diskonly")));

		return opt;
	}
}
