/*
 * Copyright 2013 Eediom Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.araqne.logstorage.script;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.araqne.api.Script;
import org.araqne.api.ScriptArgument;
import org.araqne.api.ScriptContext;
import org.araqne.api.ScriptUsage;
import org.araqne.logstorage.LogTableRegistry;
import org.araqne.logstorage.TableWildcardMatcher;
import org.araqne.logstorage.backup.StorageBackupConfigSpec;
import org.araqne.logstorage.backup.StorageBackupJob;
import org.araqne.logstorage.backup.StorageBackupManager;
import org.araqne.logstorage.backup.StorageBackupMediaFactory;
import org.araqne.logstorage.backup.StorageBackupMediaRegistry;
import org.araqne.logstorage.backup.StorageBackupRequest;
import org.araqne.logstorage.backup.StorageBackupType;
import org.araqne.logstorage.dump.DumpConfigSpec;
import org.araqne.logstorage.dump.DumpDriver;
import org.araqne.logstorage.dump.DumpService;
import org.araqne.logstorage.dump.ExportRequest;
import org.araqne.logstorage.dump.ExportTask;
import org.araqne.logstorage.dump.ImportRequest;
import org.araqne.logstorage.dump.ImportTask;

/**
 * @since 2.3.0
 * @author xeraph
 */
public class LogStorageBackupScript implements Script {
	private LogTableRegistry tableRegistry;
	private StorageBackupManager backupManager;
	private StorageBackupMediaRegistry mediaRegistry;
	private DumpService dumpService;
	private ScriptContext context;

	public LogStorageBackupScript(LogTableRegistry tableRegistry, StorageBackupManager backupManager,
			StorageBackupMediaRegistry mediaRegistry, DumpService dumpService) {
		this.tableRegistry = tableRegistry;
		this.backupManager = backupManager;
		this.mediaRegistry = mediaRegistry;
		this.dumpService = dumpService;
	}

	@Override
	public void setScriptContext(ScriptContext context) {
		this.context = context;
	}

	public void exportTasks(String[] args) {
		context.println("Export Tasks");
		context.println("--------------");

		for (ExportTask task : dumpService.getExportTasks()) {
			context.println(task);
		}
	}

	public void importTasks(String[] args) {
		context.println("Import Tasks");
		context.println("--------------");

		for (ImportTask task : dumpService.getImportTasks()) {
			context.println(task);
		}
	}

	public void beginExport(String[] args) {
		try {
			context.print("Tables? ");
			Set<String> tableNames = split(context.readLine());

			SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
			context.print("From (yyyyMMdd)? ");
			Date from = df.parse(context.readLine().trim());
			context.print("To (yyyyMMdd)? ");
			Date to = df.parse(context.readLine().trim());

			context.print("Dump Path? ");
			String path = context.readLine().trim();

			Map<String, String> params = new HashMap<String, String>();
			params.put("path", path);
			dumpService.beginExport(new ExportRequest("local", tableNames, from, to, params));
			context.println("export started");
		} catch (InterruptedException e) {
			context.println("");
			context.println("interrupted");
		} catch (ParseException e) {
			context.println("invalid date format");
		}
	}

	@ScriptUsage(description = "import data", arguments = { @ScriptArgument(name = "type", type = "string", description = "driver type") })
	public void beginImport(String[] args) {
		String type = args[0];
		try {
			DumpDriver driver = dumpService.getDumpDriver(type);
			if (driver == null) {
				context.println("unknown driver type: " + type);
				return;
			}

			ImportRequest req = new ImportRequest();
			for (DumpConfigSpec spec : driver.getImportSpecs()) {
				String value = input(spec);
			}

			dumpService.beginImport(req);
		} catch (InterruptedException e) {
			context.println("");
			context.println("interrupted");
		}
	}

	public void dumpDrivers(String[] args) {
		context.println("Dump Drivers");
		context.println("--------------");

		for (DumpDriver driver : dumpService.getDumpDrivers()) {
			context.println(driver.getName(Locale.ENGLISH) + ": " + driver.getDescription(Locale.ENGLISH));
		}
	}

	@ScriptUsage(description = "cancel export", arguments = { @ScriptArgument(name = "guid", type = "string", description = "the guid of export job") })
	public void cancelExport(String[] args) {
		String guid = args[0];
		dumpService.cancelExport(guid);
		context.println("cancelled");
	}

	private Set<String> split(String line) {
		Set<String> s = new HashSet<String>();
		for (String table : line.split(",")) {
			table = table.trim();
			if (!table.isEmpty())
				s.add(table);
		}
		return s;
	}

	public void backup(String[] args) throws InterruptedException, IOException {
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");

		StorageBackupRequest req = new StorageBackupRequest(StorageBackupType.BACKUP);
		BackupProgressPrinter printer = new BackupProgressPrinter(context);
		req.setProgressMonitor(printer);

		configureMedia(req);

		Set<String> tableNames = new HashSet<String>();
		context.print("Table names (enter to backup all tables): ");
		String tableExpr = context.readLine();
		HashSet<String> allTableNames = new HashSet<String>(tableRegistry.getTableNames());
		if (tableExpr.isEmpty()) {
			tableNames = allTableNames;
		} else {
			tableNames = TableWildcardMatcher.apply(allTableNames, tableExpr);
		}

		req.setTableNames(tableNames);

		context.print("Range from (yyyyMMdd, enter to unlimited): ");
		String fromStr = context.readLine().trim();
		Date from = df.parse(fromStr, new ParsePosition(0));
		if (!fromStr.isEmpty() && from == null) {
			context.println("invalid date format");
			return;
		}

		req.setFrom(from);

		context.print("Range to (yyyyMMdd, enter to unlimited): ");
		String toStr = context.readLine().trim();
		Date to = df.parse(toStr, new ParsePosition(0));
		if (!toStr.isEmpty() && to == null) {
			context.println("invalid date format");
			return;
		}

		req.setTo(to);

		StorageBackupJob job = backupManager.prepare(req);
		int tableCount = job.getStorageFiles().keySet().size();
		context.println("Total " + tableCount + " tables");
		context.println("Requires " + formatNumber(job.getTotalBytes()) + " bytes");

		long freeSpace = req.getMedia().getFreeSpace();
		if (freeSpace < job.getTotalBytes()) {
			context.println("Not enough space on media, Aborted");
			return;
		}

		context.print("Proceed? (y/N): ");
		String proceed = context.readLine();
		if (!proceed.equalsIgnoreCase("y")) {
			context.println("cancelled");
			return;
		}

		backupManager.execute(job);
		context.println("started backup job");

		try {
			while (true) {
				context.readLine();
			}
		} catch (InterruptedException e) {
			if (!job.isDone())
				context.println("backup will be continued in background");
		} finally {
			printer.setDisabled(true);
		}
	}

	private void configureMedia(StorageBackupRequest req) throws InterruptedException {
		Set<String> mediaTypes = new HashSet<String>();
		for (StorageBackupMediaFactory f : mediaRegistry.getFactories()) {
			mediaTypes.add(f.getName());
		}
		context.println("Available backup media types " + mediaTypes);
		context.print("Select media type: ");

		String type = context.readLine().trim();
		if (!mediaTypes.contains(type))
			throw new IllegalStateException("no such media: " + type);

		StorageBackupMediaFactory mediaFactory = mediaRegistry.getFactory(type);
		Map<String, String> mediaConfigs = new HashMap<String, String>();

		for (StorageBackupConfigSpec spec : mediaFactory.getConfigSpecs()) {
			String displayName = spec.getDisplayNames().get(Locale.ENGLISH);
			context.print(displayName + ": ");
			mediaConfigs.put(spec.getKey(), context.readLine());
		}

		req.setMedia(mediaFactory.newMedia(mediaConfigs));
	}

	public void restore(String[] args) throws InterruptedException, IOException {
		StorageBackupRequest req = new StorageBackupRequest(StorageBackupType.RESTORE);
		BackupProgressPrinter printer = new BackupProgressPrinter(context);
		req.setProgressMonitor(printer);

		configureMedia(req);

		context.print("Table names (enter to restore all tables): ");
		String tableExpr = context.readLine().trim();

		Set<String> allTableNames = req.getMedia().getTableNames();
		if (tableExpr.isEmpty()) {
			req.setTableNames(allTableNames);
		} else {
			req.setTableNames(TableWildcardMatcher.apply(allTableNames, tableExpr));
		}

		StorageBackupJob job = backupManager.prepare(req);
		int tableCount = job.getMediaFiles().keySet().size();
		context.println("Total " + tableCount + " tables");
		context.println("Restore " + formatNumber(job.getTotalBytes()) + " bytes");

		context.print("Proceed? (y/N): ");
		String proceed = context.readLine();
		if (!proceed.equalsIgnoreCase("y")) {
			context.println("cancelled");
			return;
		}

		backupManager.execute(job);
		context.println("started restore job");

		try {
			while (true) {
				context.readLine();
			}
		} catch (InterruptedException e) {
			if (!job.isDone())
				context.println("restore will be continued in background");
		} finally {
			printer.setDisabled(true);
		}
	}

	private String formatNumber(long bytes) {
		DecimalFormat formatter = new DecimalFormat("###,###");
		return formatter.format(bytes);
	}

	private String input(DumpConfigSpec spec) throws InterruptedException {
		String s = spec.isRequired() ? " (required)?" : " (optional)?";
		String value = null;
		while (true) {
			context.println(spec.getDisplayName(Locale.ENGLISH) + s);
			value = context.readLine();
			if (value.trim().isEmpty()) {
				if (spec.isRequired())
					continue;
				return null;
			}
			break;
		}

		return value;
	}
}
