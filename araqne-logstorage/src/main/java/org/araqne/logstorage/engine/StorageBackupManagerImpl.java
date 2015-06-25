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
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Requires;
import org.araqne.logstorage.LogStorage;
import org.araqne.logstorage.LogTableRegistry;
import org.araqne.logstorage.StorageConfig;
import org.araqne.logstorage.TableSchema;
import org.araqne.logstorage.backup.StorageBackupJob;
import org.araqne.logstorage.backup.StorageBackupManager;
import org.araqne.logstorage.backup.StorageBackupMedia;
import org.araqne.logstorage.backup.StorageBackupProgressMonitor;
import org.araqne.logstorage.backup.StorageBackupRequest;
import org.araqne.logstorage.backup.StorageBackupType;
import org.araqne.logstorage.backup.StorageFile;
import org.araqne.logstorage.backup.StorageMediaFile;
import org.araqne.logstorage.backup.StorageTransferRequest;
import org.araqne.logstorage.backup.StorageTransferStream;
import org.araqne.logstorage.file.DatapathUtil;
import org.araqne.storage.api.FilePath;
import org.araqne.storage.api.StorageManager;
import org.araqne.storage.localfile.LocalFilePath;
import org.json.JSONConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since 2.2.7
 * @author xeraph
 * 
 */
@Component(name = "logstorage-backup-manager")
@Provides
public class StorageBackupManagerImpl implements StorageBackupManager {
	private static final String TABLE_METADATA_JSON = "table-metadata.json";
	private final Logger logger = LoggerFactory.getLogger(StorageBackupManagerImpl.class);

	@Requires
	private LogTableRegistry tableRegistry;

	@Requires
	private LogStorage storage;

	@Requires
	private StorageManager storageManager;

	private ConcurrentHashMap<String, BackupRunner> backupRunners = new ConcurrentHashMap<String, BackupRunner>();
	private ConcurrentHashMap<String, RestoreRunner> restoreRunners = new ConcurrentHashMap<String, RestoreRunner>();

	public StorageBackupManagerImpl() {
	}

	@Invalidate
	public void stop() {
		backupRunners.clear();
		restoreRunners.clear();
	}

	@Override
	public StorageBackupJob prepare(StorageBackupRequest req) throws IOException {
		if (req.getType() == StorageBackupType.BACKUP)
			return prepareBackup(req);
		else
			return prepareRestore(req);
	}

	private StorageBackupJob prepareBackup(StorageBackupRequest req) {
		boolean overwrite = req.isOverwrite();
		boolean incremental = req.isIncremental();
		boolean worm = req.getMedia().isWormMedia();
		if ((overwrite && incremental) || (overwrite && worm) || (incremental && worm))
			throw new IllegalArgumentException("invalid backup options [guid:" + req.getGuid() + "]");

		Map<String, List<StorageFile>> storageFiles = new HashMap<String, List<StorageFile>>();
		long totalBytes = 0;
		for (String tableName : req.getTableNames()) {
			List<StorageFile> files = new ArrayList<StorageFile>();

			for (Date day : storage.getLogDates(tableName)) {
				if ((req.getFrom() != null && day.before(req.getFrom())) || (req.getTo() != null && day.after(req.getTo())))
					continue;

				TableSchema schema = tableRegistry.getTableSchema(tableName, true);
				int tableId = schema.getId();
				String basePath = schema.getPrimaryStorage().getBasePath();
				FilePath baseDir = storageManager.resolveFilePath(basePath);
				if (baseDir == null)
					baseDir = storage.getDirectory();

				if (!(baseDir instanceof LocalFilePath)) {
					logger.warn("araqne logstorage : unsupported base path : " + basePath);
					continue;
				}

				totalBytes += addStorageFile(files, tableName, tableId,
						((LocalFilePath) DatapathUtil.getIndexFile(tableId, day, baseDir)).getFile());
				totalBytes += addStorageFile(files, tableName, tableId,
						((LocalFilePath) DatapathUtil.getDataFile(tableId, day, baseDir)).getFile());
				totalBytes += addStorageFile(files, tableName, tableId,
						((LocalFilePath) DatapathUtil.getKeyFile(tableId, day, baseDir)).getFile());
			}

			storageFiles.put(tableName, files);
		}

		StorageBackupJob job = new StorageBackupJob();
		job.setRequest(req);
		job.setStorageFiles(storageFiles);
		job.setTotalBytes(totalBytes);
		return job;
	}

	public StorageBackupJob prepareRestore(StorageBackupRequest req) throws IOException {
		Map<String, List<StorageMediaFile>> targetFiles = new HashMap<String, List<StorageMediaFile>>();
		long totalBytes = 0;

		StorageBackupMedia media = req.getMedia();

		Set<String> remoteTables = media.getTableNames();
		Set<String> reqTables = req.getTableNames();

		for (String t : reqTables) {
			if (remoteTables.contains(t)) {
				List<StorageMediaFile> files = media.getFiles(t);
				for (StorageMediaFile f : files)
					totalBytes += f.getLength();

				targetFiles.put(t, files);
			}
		}

		StorageBackupJob job = new StorageBackupJob();
		job.setRequest(req);
		job.setMediaFiles(targetFiles);
		job.setTotalBytes(totalBytes);
		return job;
	}

	private long addStorageFile(List<StorageFile> files, String tableName, int tableId, File f) {
		if (!f.exists())
			return 0;

		StorageFile bf = new StorageFile(tableName, tableId, f);
		files.add(bf);
		return bf.getLength();
	}

	@Override
	public void execute(StorageBackupJob job) {
		if (job == null)
			throw new IllegalArgumentException("job should not be null");

		if (job.getRequest().getType() == StorageBackupType.BACKUP) {
			BackupRunner runner = new BackupRunner(job);
			job.setSubmitAt(new Date());

			String guid = job.getRequest().getGuid();
			BackupRunner old = backupRunners.putIfAbsent(guid, runner);
			if (old != null) {
				backupRunners.remove(guid, old);
				backupRunners.putIfAbsent(guid, runner);
			}
			runner.start();
		} else {
			RestoreRunner runner = new RestoreRunner(job);
			job.setSubmitAt(new Date());

			String guid = job.getRequest().getGuid();
			RestoreRunner old = restoreRunners.putIfAbsent(guid, runner);
			if (old != null) {
				restoreRunners.remove(guid, old);
				restoreRunners.putIfAbsent(guid, runner);
			}
			runner.start();
		}
	}

	@Override
	public List<StorageBackupJob> getBackupJobs() throws CloneNotSupportedException {
		List<StorageBackupJob> backupJobs = new ArrayList<StorageBackupJob>();
		for (BackupRunner r : backupRunners.values()) {
			backupJobs.add((StorageBackupJob) r.job.clone());
		}

		return backupJobs;
	}

	@Override
	public List<StorageBackupJob> getRestoreJobs() throws CloneNotSupportedException {
		List<StorageBackupJob> restoreJobs = new ArrayList<StorageBackupJob>();
		for (RestoreRunner r : restoreRunners.values()) {
			restoreJobs.add((StorageBackupJob) r.job.clone());
		}

		return restoreJobs;
	}

	@Override
	public StorageBackupJob getBackupJob(String guid) throws CloneNotSupportedException {
		BackupRunner r = backupRunners.get(guid);
		return (r == null) ? null : (StorageBackupJob) r.job.clone();
	}

	@Override
	public StorageBackupJob getRestoreJob(String guid) throws CloneNotSupportedException {
		RestoreRunner r = restoreRunners.get(guid);
		return (r == null) ? null : (StorageBackupJob) r.job.clone();
	}

	@Override
	public void cancel(String guid) {
		BackupRunner r = backupRunners.get(guid);
		r.tr.cancelCopy();
	}

	private class RestoreRunner extends Thread {
		private final StorageBackupJob job;

		public RestoreRunner(StorageBackupJob job) {
			super("LogStorage Restore");
			this.job = job;
		}

		@Override
		public void run() {
			StorageBackupMedia media = job.getRequest().getMedia();
			StorageBackupProgressMonitor monitor = job.getRequest().getProgressMonitor();
			if (monitor != null)
				monitor.onBeginJob(job);

			try {
				Set<String> tableNames = job.getMediaFiles().keySet();

				for (String tableName : tableNames) {
					if (monitor != null)
						monitor.onBeginTable(job, tableName);

					// TODO : handle non-local table directory
					FilePath tableDir = storage.getTableDirectory(tableName);
					if (tableDir == null || !(tableDir instanceof LocalFilePath)) {
						logger.warn("araqne logstorage: cannot handle table " + tableName + ". skip...");
						continue;
					}

					// restore table metadata
					try {
						Map<String, String> metadata = media.getTableMetadata(tableName);
						String type = metadata.get("_filetype");
						if (type == null)
							throw new IOException("storage type not found for table " + tableName);

						if (!tableRegistry.exists(tableName)) {
							TableSchema schema = new TableSchema();
							schema.setName(tableName);
							schema.setPrimaryStorage(new StorageConfig(type));
							schema.setMetadata(metadata);

							tableRegistry.createTable(schema);
						}
					} catch (IOException e) {
						if (monitor != null)
							monitor.onCompleteTable(job, tableName);

						logger.error("araqne logstorage: cannot read backup table metadata", e);
						continue;
					}

					// transfer files
					int tableId = tableRegistry.getTableSchema(tableName, true).getId();
					List<StorageMediaFile> files = job.getMediaFiles().get(tableName);

					for (StorageMediaFile mediaFile : files) {
						String storageFilename = new File(mediaFile.getFileName()).getName();

						File storageFilePath = ((LocalFilePath) tableDir.newFilePath(storageFilename)).getFile();
						StorageFile storageFile = new StorageFile(tableName, tableId, storageFilePath);
						StorageTransferRequest tr = new StorageTransferRequest(storageFile, mediaFile);
						try {
							if (monitor != null)
								monitor.onBeginFile(job, tableName, mediaFile.getFileName(), mediaFile.getLength());

							media.copyFromMedia(tr);
						} catch (IOException e) {
							mediaFile.setException(e);
							if (logger.isDebugEnabled())
								logger.debug("araqne logstorage: restore failed", e);
						} finally {
							mediaFile.setDone(true);

							if (monitor != null)
								monitor.onCompleteFile(job, tableName, mediaFile.getFileName(), mediaFile.getLength(),
										mediaFile.getException());
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
		private final StorageBackupJob job;
		private StorageTransferRequest tr;

		public BackupRunner(StorageBackupJob job) {
			super("LogStorage Backup");
			this.job = job;
		}

		@Override
		public void run() {
			StorageBackupMedia media = job.getRequest().getMedia();
			StorageBackupProgressMonitor monitor = job.getRequest().getProgressMonitor();
			if (monitor != null)
				monitor.onBeginJob(job);

			try {
				Set<String> tableNames = job.getStorageFiles().keySet();
				for (String tableName : tableNames) {
					List<StorageFile> files = job.getStorageFiles().get(tableName);

					if (!checkValidation(job, media, tableName, files))
						throw new IOException("backup file already exists");

					if (monitor != null)
						monitor.onBeginTable(job, tableName);

					if (!media.isWormMedia()) {
						try {
							TableSchema schema = tableRegistry.getTableSchema(tableName);

							// overwrite table metadata file
							Map<String, Object> metadata = new HashMap<String, Object>();
							metadata.put("table_name", tableName);
							metadata.put("metadata", schema.getMetadata());

							String json = JSONConverter.jsonize(metadata);
							byte[] b = json.getBytes("utf-8");
							ByteArrayInputStream is = new ByteArrayInputStream(b);
							int tableId = schema.getId();
							StorageTransferStream stream = new StorageTransferStream(tableName, tableId, is, TABLE_METADATA_JSON);
							media.copyToMedia(new StorageTransferRequest(stream));
						} catch (Exception e) {
							logger.error("araqne logstorage: table metadata backup failed", e);
						}
					}

					// transfer files
					for (StorageFile storageFile : files) {
						if (storageFile.getLength() > media.getFreeSpace())
							throw new IOException("not enough media space: free " + media.getFreeSpace() + ", required: "
									+ storageFile.getLength());

						String subPath = storageFile.getFile().getParentFile().getName() + File.separator
								+ storageFile.getFile().getName();
						StorageMediaFile mediaFile = new StorageMediaFile(tableName, subPath, storageFile.getLength());
						tr = new StorageTransferRequest(storageFile, mediaFile);
						tr.setOverwrite(job.getRequest().isOverwrite());
						tr.setIncremental(job.getRequest().isIncremental());
						try {
							if (monitor != null)
								monitor.onBeginFile(job, tableName, storageFile.getFileName(), storageFile.getLength());

							media.copyToMedia(tr);
						} catch (IOException e) {
							storageFile.setException(e);
							if (logger.isDebugEnabled())
								logger.debug("araqne logstorage: table backup failed", e);
						} finally {
							storageFile.setDone(true);
							if (job.getRequest().isMove())
								tryDelete(media, tableName, storageFile);

							if (monitor != null)
								monitor.onCompleteFile(job, tableName, storageFile.getFileName(), storageFile.getLength(),
										storageFile.getException());
						}
					}

					if (monitor != null)
						monitor.onCompleteTable(job, tableName);
				}
			} catch (Throwable t) {
				job.setErrorCause(t);
				logger.error("araqne logstorage: backup job failed", t);
			} finally {
				generateReport(job);
				job.setDone(true);
				backupRunners.remove(job.getRequest().getGuid(), job);

				if (monitor != null)
					monitor.onCompleteJob(job);
			}
		}

		private void tryDelete(StorageBackupMedia media, String tableName, StorageFile storageFile) throws IOException {
			File backupFile = new File(job.getTablePath(), "table/" + storageFile.getTableId() + "/" + storageFile.getFileName());
			File src = storageFile.getFile();
			if (src.length() == backupFile.length()) {
				boolean isDelete = src.delete();
				if (!isDelete)
					throw new IOException("delete failed [file:" + src.getAbsolutePath() + "]");
			} else
				throw new IOException("move failed [original size: " + src.length() + ", backup size: " + backupFile.length()
						+ "]");
		}
	}

	private void generateReport(StorageBackupJob job) {
		String type = job.getRequest().getType().toString().toLowerCase();

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
				tables = job.getStorageFiles().keySet();
			else
				tables = job.getMediaFiles().keySet();

			for (String table : tables) {
				writeLine(bw, "Table [" + table + "]");

				if (type.equals("backup")) {
					for (StorageFile bf : job.getStorageFiles().get(table)) {
						String path = bf.getFile().getAbsolutePath();
						writeReportLog(bw, path, bf.getLength(), bf.getException());
					}
				} else {
					for (StorageMediaFile bf : job.getMediaFiles().get(table)) {
						String path = bf.getFileName();
						writeReportLog(bw, path, bf.getLength(), bf.getException());
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

	private void writeReportLog(BufferedWriter bw, String path, long length, Throwable t) throws IOException {
		if (t == null)
			writeLine(bw, "[O] " + path + ":" + length);
		else
			writeLine(bw, "[X] " + path + ":" + length + ":" + t.getMessage());
	}

	private void writeLine(Writer writer, String line) throws IOException {
		String sep = System.getProperty("line.separator");
		writer.write(line + sep);
	}

	private boolean checkValidation(StorageBackupJob job, StorageBackupMedia media, String tableName, List<StorageFile> files)
			throws IOException {
		StorageBackupRequest req = job.getRequest();
		if (req.isOverwrite() || req.isIncremental() || media.isWormMedia())
			return true;

		if (files.size() == 0)
			return true;

		for (StorageFile f : files) {
			if (media.exists(tableName, f.getFileName()))
				return false;
		}
		return true;
	}
}
