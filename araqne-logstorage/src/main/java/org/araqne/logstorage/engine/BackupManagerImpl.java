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
package org.araqne.logstorage.engine;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogTableRegistry;
import org.araqne.logstorage.backup.BackupJob;
import org.araqne.logstorage.backup.BackupManager;
import org.araqne.logstorage.backup.BackupMedia;
import org.araqne.logstorage.backup.JobProgressMonitor;
import org.araqne.logstorage.backup.BackupRequest;
import org.araqne.logstorage.backup.BaseFile;
import org.araqne.logstorage.backup.Job;
import org.araqne.logstorage.backup.MediaFile;
import org.araqne.logstorage.backup.RestoreJob;
import org.araqne.logstorage.backup.RestoreRequest;
import org.araqne.logstorage.backup.StorageFile;
import org.araqne.logstorage.backup.TransferRequest;
import org.json.JSONConverter;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 2.2.7
 * @author xeraph
 * 
 */
@Component(name = "logstorage-backup-manager")
@Provides
public class BackupManagerImpl implements BackupManager {
	private static final String TABLE_METADATA_JSON = "table-metadata.json";
	private final Logger logger = LoggerFactory.getLogger(BackupManagerImpl.class);

	@Requires
	private LogTableRegistry tableRegistry;

	@Requires
	private LogStorage storage;

	private List<BackupRunner> backupRunners = Collections.synchronizedList(new ArrayList<BackupRunner>());
	private List<RestoreRunner> restoreRunners = Collections.synchronizedList(new ArrayList<RestoreRunner>());

	@Override
	public BackupJob prepareBackup(BackupRequest req) {
		Map<String, List<StorageFile>> storageFiles = new HashMap<String, List<StorageFile>>();
		long totalBytes = 0;
		for (String tableName : req.getTableNames()) {
			List<StorageFile> files = new ArrayList<StorageFile>();

			for (Date day : storage.getLogDates(tableName)) {
				if ((req.getFrom() != null && day.before(req.getFrom())) ||
						(req.getTo() != null && day.after(req.getTo())))
					continue;

				int tableId = tableRegistry.getTableId(tableName);
				String basePath = tableRegistry.getTableMetadata(tableName, "base_path");

				totalBytes += addStorageFile(files, tableId, tableName, DatapathUtil.getIndexFile(tableId, day, basePath));
				totalBytes += addStorageFile(files, tableId, tableName, DatapathUtil.getDataFile(tableId, day, basePath));
				totalBytes += addStorageFile(files, tableId, tableName, DatapathUtil.getKeyFile(tableId, day, basePath));
			}

			storageFiles.put(tableName, files);
		}

		BackupJob job = new BackupJob();
		job.setRequest(req);
		job.setSourceFiles(storageFiles);
		job.setTotalBytes(totalBytes);

		return job;
	}

	@Override
	public RestoreJob prepareRestore(RestoreRequest req) throws IOException {
		Map<String, List<MediaFile>> targetFiles = new HashMap<String, List<MediaFile>>();
		long totalBytes = 0;

		BackupMedia media = req.getMedia();

		Set<String> remoteTables = media.getTableNames();
		Set<String> reqTables = req.getTableNames();

		for (String t : reqTables) {
			if (remoteTables.contains(t)) {
				List<MediaFile> files = media.getFiles(t);
				for (BaseFile f : files)
					totalBytes += f.getLength();

				targetFiles.put(t, files);
			}
		}

		RestoreJob job = new RestoreJob();
		job.setRequest(req);
		job.setSourceFiles(targetFiles);
		job.setTotalBytes(totalBytes);

		return job;
	}

	private long addStorageFile(List<StorageFile> files, int tableId, String tableName, File f) {
		if (!f.exists())
			return 0;

		StorageFile bf = new StorageFile(tableName, f);
		files.add(bf);
		return bf.getLength();
	}

	@Override
	public void execute(BackupJob job) {
		if (job == null)
			throw new IllegalArgumentException("job should not be null");

		BackupRunner runner = new BackupRunner(job);
		job.setSubmitAt(new Date());
		runner.start();
	}

	@Override
	public void execute(RestoreJob job) {
		if (job == null)
			throw new IllegalArgumentException("job should not be null");

		RestoreRunner runner = new RestoreRunner(job);
		job.setSubmitAt(new Date());
		runner.start();
	}

	@Override
	public List<BackupJob> getBackupJobs() {
		ArrayList<BackupJob> jobs = new ArrayList<BackupJob>();

		for (BackupRunner runner : backupRunners) {
			jobs.add(runner.job);
		}

		return jobs;
	}

	@Override
	public List<RestoreJob> getRestoreJobs() {
		ArrayList<RestoreJob> jobs = new ArrayList<RestoreJob>();

		for (RestoreRunner runner : restoreRunners) {
			jobs.add(runner.job);
		}

		return jobs;
	}

	private class RestoreRunner extends Thread {
		private final RestoreJob job;

		public RestoreRunner(RestoreJob job) {
			super("LogStorage Restore");
			this.job = job;
		}

		@Override
		public void run() {
			BackupMedia media = job.getRequest().getMedia();
			JobProgressMonitor monitor = job.getRequest().getProgressMonitor();
			if (monitor != null)
				monitor.onBeginJob(job);

			try {
				Set<String> tableNames = job.getSourceFiles().keySet();

				for (String tableName : tableNames) {
					// restore table metadata
					try {
						Map<String, String> metadata = media.getTableMetadata(tableName);
						String type = metadata.get("_filetype");
						if (type == null)
							throw new IOException("storage type not found for table " + tableName);

						if (!tableRegistry.exists(tableName)) {
							tableRegistry.createTable(tableName, type, metadata);
						}
					} catch (IOException e) {
						logger.error("araqne logstorage: cannot read backup table metadata", e);
						continue;
					}

					// transfer files
					List<MediaFile> files = job.getSourceFiles().get(tableName);

					for (MediaFile mediaFile : files) {
						String storageFilename = new File(mediaFile.getFileName()).getName(); // omit old table id
						File storageFilePath = new File(storage.getTableDirectory(tableName), storageFilename);
						StorageFile storageFile = new StorageFile(tableName, storageFilePath);
						TransferRequest tr = new TransferRequest(storageFile, mediaFile);
						try {
							if (monitor != null)
								monitor.onBeginFile(job, mediaFile);

							media.copyFromMedia(tr);
						} catch (IOException e) {
							mediaFile.setException(e);
							if (logger.isDebugEnabled())
								logger.debug("araqne logstorage: restore failed", e);
						} finally {
							mediaFile.setDone(true);

							if (monitor != null)
								monitor.onCompleteFile(job, mediaFile);
						}
					}

					if (monitor != null)
						monitor.onCompleteTable(job, tableName);
				}

			} finally {
				generateReport(job);

				job.setDone(true);

				if (monitor != null)
					monitor.onCompleteJob(job);
			}
		}
	}

	private class BackupRunner extends Thread {
		private final BackupJob job;

		public BackupRunner(BackupJob job) {
			super("LogStorage Backup");
			this.job = job;
		}

		@Override
		public void run() {
			BackupMedia media = job.getRequest().getMedia();
			JobProgressMonitor monitor = job.getRequest().getProgressMonitor();
			if (monitor != null)
				monitor.onBeginJob(job);

			try {
				Set<String> tableNames = job.getSourceFiles().keySet();

				for (String tableName : tableNames) {
					if (monitor != null)
						monitor.onBeginTable(job, tableName);

					// overwrite table metadata file
					Map<String, Object> metadata = new HashMap<String, Object>();
					metadata.put("_tablename", tableName);
					Map<String, String> tableMetadata = new HashMap<String, String>();
					for (String key : tableRegistry.getTableMetadataKeys(tableName)) {
						tableMetadata.put(key, tableRegistry.getTableMetadata(tableName, key));
					}
					metadata.put("metadata", tableMetadata);

					try {
						String json = JSONConverter.jsonize(metadata);
						byte[] b = json.getBytes("utf-8");
						ByteArrayInputStream is = new ByteArrayInputStream(b);
						media.copyToMedia(new TransferRequest(tableName, is, TABLE_METADATA_JSON));
					} catch (Exception e) {
						logger.error("araqne logstorage: table metadata backup failed", e);
					}

					// transfer files
					List<StorageFile> files = job.getSourceFiles().get(tableName);

					for (StorageFile storageFile : files) {
						String subPath = storageFile.getFile().getParentFile().getName() + File.separator + storageFile.getFile().getName();
						MediaFile mediaFile = new MediaFile(tableName, subPath, storageFile.getLength());
						TransferRequest tr = new TransferRequest(storageFile, mediaFile);
						try {
							if (monitor != null)
								monitor.onBeginFile(job, storageFile);

							media.copyToMedia(tr);
						} catch (IOException e) {
							storageFile.setException(e);
							if (logger.isDebugEnabled())
								logger.debug("araqne logstorage: table backup failed", e);
						} finally {
							storageFile.setDone(true);

							if (monitor != null)
								monitor.onCompleteFile(job, storageFile);
						}
					}

					if (monitor != null)
						monitor.onCompleteTable(job, tableName);
				}
			} catch (Throwable t) {
				logger.error("araqne logstorage: backup job failed", t);
			} finally {
				generateReport(job);

				job.setDone(true);

				if (monitor != null)
					monitor.onCompleteJob(job);
			}
		}

	}

	private void generateReport(Job job) {
		String type = job.getRequest() instanceof BackupRequest ? "backup" : "restore";

		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd_HHmm");
		SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");

		File dir = new File(System.getProperty("araqne.log.dir"));
		File reportFile = new File(dir, type + "-" + df.format(job.getSubmitAt()) + "-report.txt");

		FileOutputStream fos = null;
		BufferedWriter bw = null;
		try {
			fos = new FileOutputStream(reportFile);
			bw = new BufferedWriter(new OutputStreamWriter(fos, "utf-8"));

			writeLine(bw, "LOGSTORAGE " + type.toUpperCase() + " REPORT");
			writeLine(bw, "==========================");

			writeLine(bw, "");
			writeLine(bw, "Submit: " + df2.format(job.getSubmitAt()));
			writeLine(bw, "Total: " + job.getTotalBytes() + " bytes");
			writeLine(bw, "");

			Set<String> tables = null;
			if (type.equals("backup"))
				tables = ((BackupJob) job).getSourceFiles().keySet();
			else
				tables = ((RestoreJob) job).getSourceFiles().keySet();

			for (String table : tables) {
				writeLine(bw, "Table [" + table + "]");

				if (type.equals("backup")) {
					for (StorageFile bf : ((BackupJob) job).getSourceFiles().get(table)) {
						String path = bf.getFile().getAbsolutePath();
						writeReportLog(bw, bf, path);
					}
				} else {
					for (MediaFile bf : ((RestoreJob) job).getSourceFiles().get(table)) {
						String path = bf.getFileName();
						writeReportLog(bw, bf, path);
					}
				}

				writeLine(bw, "");
			}
		} catch (IOException e) {
			logger.error("araqne logstorage: cannot generate " + type + " report", e);
		} finally {
			if (bw != null) {
				try {
					bw.close();
				} catch (IOException e) {
				}
			}

			if (fos != null) {
				try {
					fos.close();
				} catch (IOException e) {
				}
			}
		}
	}

	private void writeReportLog(BufferedWriter bw, BaseFile bf, String path) throws IOException {
		if (bf.getException() == null)
			writeLine(bw, "[O] " + path + ":" + bf.getLength());
		else
			writeLine(bw, "[X] " + path + ":" + bf.getLength() + ":" + bf.getException().getMessage());
	}

	private void writeLine(Writer writer, String line) throws IOException {
		String sep = System.getProperty("line.separator");
		writer.write(line + sep);
	}
}
