package org.araqne.logdb.metadata;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.araqne.logdb.AccountService;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.Privilege;
import org.araqne.logstorage.LogTableRegistry;

public class MetadataQueryStringParser {
	public static List<String> getTableNames(LogQueryContext context, LogTableRegistry tableRegistry,
			AccountService accountService, String token) {
		HashSet<String> tableFilter = null;
		String[] argTableNames = null;
		if (!token.trim().isEmpty()) {
			tableFilter = new HashSet<String>();
			argTableNames = token.split(",");

			for (int i = 0; i < argTableNames.length; i++)
				tableFilter.add(argTableNames[i].trim());
		}

		List<String> tableNames = new ArrayList<String>();

		if (context.getSession().getLoginName().equals("araqne")) {
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

		return tableNames;
	}

}
