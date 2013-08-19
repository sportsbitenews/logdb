package org.araqne.logdb.metadata;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import org.araqne.logdb.AccountService;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.LogQueryParseException;
import org.araqne.logdb.Privilege;
import org.araqne.logstorage.LogTableRegistry;

public class MetadataQueryStringParser {
	public static TableScanOption getTableNames(LogQueryContext context, LogTableRegistry tableRegistry,
			AccountService accountService, String token) {
		token = token.trim();
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");

		int b = -1;
		int e = -1;
		Date from = null;

		b = token.indexOf("from=");
		if (b >= 0) {
			e = token.indexOf(' ', b + 5);
			if (e < 0)
				e = token.length();

			from = df.parse(token.substring(b + 5, e), new ParsePosition(0));
			if (from == null)
				throw new LogQueryParseException("invalid-from", -1);
		}

		Date to = null;
		b = token.indexOf("to=");
		if (b >= 0) {
			e = token.indexOf(' ', b + 3);
			if (e < 0)
				e = token.length();

			to = df.parse(token.substring(b + 3, e), new ParsePosition(0));
			if (to == null)
				throw new LogQueryParseException("invalid-to", -1);
		}

		if (to != null && from != null && to.before(from))
			throw new LogQueryParseException("invalid-date-range", -1);

		if (e >= 0)
			token = token.substring(e).trim();

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
		return opt;
	}

}
