package org.araqne.logdb.query.command;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.AccountService;
import org.araqne.logdb.LogMap;
import org.araqne.logdb.LogQueryCommand;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.Privilege;
import org.araqne.logstorage.LogTableRegistry;

public class Logdb extends LogQueryCommand {
	private LogQueryContext context;
	private String objectType;
	private LogTableRegistry tableRegistry;
	private AccountService accountService;

	public Logdb(LogQueryContext context, String objectType, LogTableRegistry tableRegistry, AccountService accountService) {
		this.context = context;
		this.objectType = objectType;
		this.tableRegistry = tableRegistry;
		this.accountService = accountService;
	}

	@Override
	public void start() {
		status = Status.Running;

		try {
			if (objectType.equals("tables")) {
				if (context.getSession().getLoginName().equals("araqne")) {
					for (String tableName : tableRegistry.getTableNames()) {
						writeTableInfo(tableName);
					}
				} else {
					List<Privilege> privileges = accountService.getPrivileges(context.getSession(), context.getSession()
							.getLoginName());
					for (Privilege p : privileges) {
						if (p.getPermissions().size() > 0 && tableRegistry.exists(p.getTableName())) {
							writeTableInfo(p.getTableName());
						}
					}
				}
			}
		} finally {
			eof();
		}
	}

	private void writeTableInfo(String tableName) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("table", tableName);
		write(new LogMap(m));
	}

	@Override
	public void push(LogMap m) {
	}

	@Override
	public boolean isReducer() {
		return false;
	}

}
