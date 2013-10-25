package org.araqne.logstorage.script;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.araqne.api.Script;
import org.araqne.api.ScriptContext;
import org.araqne.logstorage.LogTableRegistry;
import org.araqne.logstorage.TableWildcardMatcher;
import org.araqne.logstorage.backup.StorageBackupConfigSpec;
import org.araqne.logstorage.backup.StorageBackupJob;
import org.araqne.logstorage.backup.StorageBackupManager;
import org.araqne.logstorage.backup.StorageBackupMediaFactory;
import org.araqne.logstorage.backup.StorageBackupMediaRegistry;
import org.araqne.logstorage.backup.StorageBackupRequest;
import org.araqne.logstorage.backup.StorageBackupType;

public class LogStorageBackupScript implements Script {
	private LogTableRegistry tableRegistry;
	private StorageBackupManager backupManager;
	private StorageBackupMediaRegistry mediaRegistry;
	private ScriptContext context;

	public LogStorageBackupScript(LogTableRegistry tableRegistry, StorageBackupManager backupManager,
			StorageBackupMediaRegistry mediaRegistry) {
		this.tableRegistry = tableRegistry;
		this.backupManager = backupManager;
		this.mediaRegistry = mediaRegistry;
	}

	@Override
	public void setScriptContext(ScriptContext context) {
		this.context = context;
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

}
