package org.araqne.logdb.query.command;

import java.io.File;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.araqne.logdb.AccountService;
import org.araqne.logdb.LogMap;
import org.araqne.logdb.LogQueryCommand;
import org.araqne.logdb.LogQueryContext;
import org.araqne.logdb.Privilege;
import org.araqne.logstorage.LogFileService;
import org.araqne.logstorage.LogFileServiceRegistry;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogTableRegistry;

public class Logdb extends LogQueryCommand {
	private LogQueryContext context;
	private String objectType;
	private String args;
	private LogTableRegistry tableRegistry;
	private LogStorage storage;
	private AccountService accountService;
	private LogFileServiceRegistry logFileServiceRegistry;

	public Logdb(LogQueryContext context, String objectType, String args, LogTableRegistry tableRegistry, LogStorage storage,
			AccountService accountService, LogFileServiceRegistry logFileServiceRegistry) {
		this.context = context;
		this.objectType = objectType;
		this.args = args;
		this.tableRegistry = tableRegistry;
		this.storage = storage;
		this.accountService = accountService;
		this.logFileServiceRegistry = logFileServiceRegistry;
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
			} else if (objectType.equals("count")) {
				for (String tableName : getTableNames(args))
					countFiles(tableName);
			} else if (objectType.equals("logdisk")) {
				for (String tableName : getTableNames(args))
					writeLogDiskUsages(tableName);
			}
		} finally {
			eof();
		}
	}

	private List<String> getTableNames(String token) {
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

	private void writeTableInfo(String tableName) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("table", tableName);
		write(new LogMap(m));
	}

	private void countFiles(String tableName) {
		String fileType = tableRegistry.getTableMetadata(tableName, "_filetype");
		if (fileType == null)
			fileType = "v2";

		File dir = storage.getTableDirectory(tableName);
		countFiles(tableName, fileType, dir);
	}

	private void countFiles(String tableName, String type, File dir) {
		File[] files = dir.listFiles();
		if (files == null)
			return;

		LogFileService fileService = logFileServiceRegistry.getLogFileService(type);
		if (fileService == null)
			return;

		ArrayList<String> paths = new ArrayList<String>();
		for (File f : files)
			if (f.isFile())
				paths.add(f.getAbsolutePath());

		Collections.sort(paths);

		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		for (String path : paths) {
			File f = new File(path);
			if (f.getName().endsWith(".idx")) {
				long count = fileService.count(f);
				Date day = df.parse(f.getName().substring(0, f.getName().length() - 4), new ParsePosition(0));
				if (day == null)
					continue;

				writeCount(tableName, day, count);
			}
		}
	}

	private void writeCount(String tableName, Date day, long count) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("_time", day);
		m.put("table", tableName);
		m.put("count", count);
		write(new LogMap(m));
	}

	private void writeLogDiskUsages(String tableName) {
		File dir = storage.getTableDirectory(tableName);

		File[] files = dir.listFiles();
		if (files == null)
			return;

		List<String> paths = new ArrayList<String>();
		for (File f : files)
			if (f.getName().endsWith(".idx") || f.getName().endsWith(".dat"))
				paths.add(f.getAbsolutePath());

		Collections.sort(paths);

		Date lastDay = null;
		long diskUsage = 0;
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		for (String path : paths) {
			File f = new File(path);
			Date day = df.parse(f.getName().substring(0, f.getName().length() - 4), new ParsePosition(0));
			if (day == null)
				continue;

			if (lastDay != null && !lastDay.equals(day)) {
				writeDiskUsageLog(tableName, lastDay, diskUsage);
				diskUsage = 0;
			}

			diskUsage += f.length();
			lastDay = day;
		}

		writeDiskUsageLog(tableName, lastDay, diskUsage);
	}

	private void writeDiskUsageLog(String tableName, Date day, long diskUsage) {
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("_time", day);
		m.put("table", tableName);
		m.put("disk_usage", diskUsage);
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
