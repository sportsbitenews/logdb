/**
 * Copyright 2014 Eediom Inc.
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
package org.araqne.log.api.nio;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.araqne.log.api.AbstractLogPipe;
import org.araqne.log.api.AbstractLogger;
import org.araqne.log.api.CommonHelper;
import org.araqne.log.api.LastPosition;
import org.araqne.log.api.LastPositionHelper;
import org.araqne.log.api.Log;
import org.araqne.log.api.Logger;
import org.araqne.log.api.LoggerFactory;
import org.araqne.log.api.LoggerSpecification;
import org.araqne.log.api.LoggerStartReason;
import org.araqne.log.api.LoggerStatus;
import org.araqne.log.api.LoggerStopReason;
import org.araqne.log.api.MultilineLogExtractor;
import org.araqne.log.api.Reconfigurable;
import org.araqne.log.api.ScanPeriodMatcher;

public class NioRecursiveDirectoryWatchLogger extends AbstractLogger implements Reconfigurable {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(NioRecursiveDirectoryWatchLogger.class.getName());
	private int pollInterval = 0;
	private int pollTimeout = 100;

	private String basePath;
	private Pattern fileNamePattern;
	private Pattern dirPathPattern;
	private boolean recursive;
	private String fileTag;
	private String pathTag;
	private int scanDays;

	private Receiver receiver = new Receiver();

	/**
	 * NOTE: must be separate thread for accurate event processing
	 */
	private ChangeDetector detector;

	private MultilineLogExtractor extractor;
	private ScanPeriodMatcher scanPeriodMatcher;

	private boolean walkTreeRequired = true;
	private boolean walkForceStopped = false;

	// update only state is modified (reduce large file set serialization
	// overhead)
	private volatile boolean modifiedStates;

	public NioRecursiveDirectoryWatchLogger(LoggerSpecification spec, LoggerFactory factory) {
		super(spec, factory);

		loadPollConfigs();

		if (slog.isDebugEnabled())
			slog.debug("araqne-logapi-nio: recursive dirwatcher uses nio");

		applyConfig();
	}

	private void loadPollConfigs() {
		String s = System.getProperty("araqne.nio.poll_interval");
		if (s != null) {
			try {
				pollInterval = Integer.valueOf(s);
				slog.info("araqne-logapi-nio: use recursive watcher poll interval [{}]", pollInterval);
			} catch (Throwable t) {
			}
		}

		s = System.getProperty("araqne.nio.poll_timeout");
		if (s != null) {
			try {
				pollTimeout = Integer.valueOf(s);
				slog.info("araqne-logapi-nio: use recursive watcher poll timeout [{}]", pollTimeout);
			} catch (Throwable t) {
			}
		}
	}

	@Override
	protected void onResetStates() {
		walkTreeRequired = true;
		slog.debug("araqne-logapi-nio: recursive-dirwatch [{}] will retraverse directories", getFullName());
	}

	private void applyConfig() {
		basePath = getConfigs().get("base_path");

		String fileNameRegex = getConfigs().get("filename_pattern");
		fileNamePattern = Pattern.compile(fileNameRegex);

		// optional
		String dirNameRegex = getConfigs().get("dirpath_pattern");
		if (dirNameRegex != null)
			dirPathPattern = Pattern.compile(dirNameRegex);

		extractor = MultilineLogExtractor.build(this, receiver);

		// optional
		String recursive = getConfigs().get("recursive");
		this.recursive = ((recursive != null) && (recursive.compareToIgnoreCase("true") == 0));

		// optional
		this.fileTag = getConfigs().get("file_tag");

		// optional
		this.pathTag = getConfigs().get("path_tag");

		// optional
		String scanDaysString = getConfigs().get("scan_days");
		if (scanDaysString != null) {
			try {
				this.scanDays = Integer.parseInt(scanDaysString);
				if (scanDays < 0)
					slog.warn("araqne logapi nio: logger [" + getFullName()
							+ "] has invalid scan days [{}], config will be ignored.", scanDaysString);
			} catch (NumberFormatException e) {
				slog.warn("araqne logapi nio: logger [" + getFullName() + "] has invalid scan days [{}], config will be ignored.",
						scanDaysString);
			}
		}

		// optional
		String pathDateFormatString = getConfigs().get("path_date_format");
		if (pathDateFormatString != null) {
			try {
				SimpleDateFormat df = new SimpleDateFormat(pathDateFormatString, new Locale(extractor.getDateLocale()));
				this.scanPeriodMatcher = new ScanPeriodMatcher(df, extractor.getDateFormat().getTimeZone(), this.scanDays);
			} catch (Throwable t) {
				slog.warn("araqne logapi nio: logger [" + getFullName() + "] has invalid path date format ["
						+ pathDateFormatString + "], locale [" + extractor.getDateLocale() + "], timezone ["
						+ extractor.getDateFormat().getTimeZone().getDisplayName() + "]", t);
			}
		}
	}

	@Override
	protected void onStart(LoggerStartReason reason) {
		detector = new ChangeDetector();
		detector.start();
	}

	@Override
	protected void onStop(LoggerStopReason reason) {
		detector.close();
		walkTreeRequired = true;
	}

	@Override
	protected void runOnce() {
		if (detector.deadThread)
			throw new IllegalStateException("dead file watcher, logger[ [" + getFullName() + "]");

		walkForceStopped = false;
		if (walkTreeRequired) {
			try {
				Map<String, LastPosition> lastPositions = LastPositionHelper.deserialize(getStates());
				Path root = FileSystems.getDefault().getPath(basePath);
				Files.walkFileTree(root, new InitialRunner(root, lastPositions));

				// mark deleted files
				for (String path : new ArrayList<String>(lastPositions.keySet())) {
					markDeletedFile(lastPositions, new File(path));
				}

				if (modifiedStates)
					setStates(LastPositionHelper.serialize(lastPositions));
				else
					slog.debug("araqne-logapi-nio: logger [{}] has no modification, skip setStates()", getFullName());

				if (!walkForceStopped)
					walkTreeRequired = false;
			} catch (IOException e) {
				throw new IllegalStateException("failed to initial run, logger [" + getFullName() + "]", e);
			}
		}

		CommonHelper helper = new CommonHelper(dirPathPattern, fileNamePattern, scanPeriodMatcher);
		Map<String, LastPosition> lastPositions = null;
		try {
			// avoid unnecessary processing
			if (scanPeriodMatcher != null)
				lastPositions = LastPositionHelper.deserialize(getStates());

			List<File> changedFiles = new ArrayList<File>(detector.getChangedFiles());
			List<File> deletedFiles = new ArrayList<File>(detector.getDeletedFiles());

			if (changedFiles.isEmpty() && deletedFiles.isEmpty()) {
				return;
			}

			lastPositions = LastPositionHelper.deserialize(getStates());

			Collections.sort(changedFiles);
			for (File f : changedFiles) {
				processFile(lastPositions, f, helper);
			}

			Collections.sort(deletedFiles);
			for (File f : deletedFiles) {
				markDeletedFile(lastPositions, f);
			}
		} finally {
			helper.removeOutdatedStates(lastPositions);

			if (lastPositions != null && modifiedStates)
				setStates(LastPositionHelper.serialize(lastPositions));
			else
				slog.debug("araqne-logapi-nio: logger [{}] has no modification, skip setStates()", getFullName());

			modifiedStates = false;
		}
	}

	private void markDeletedFile(Map<String, LastPosition> lastPositions, File f) {
		if (f.exists())
			return;

		String path = f.getAbsolutePath();
		LastPosition lp = lastPositions.get(path);
		if (lp == null)
			return;

		modifiedStates = true;
		if (lp.getLastSeen() == null) {
			lp.setLastSeen(new Date());
			slog.debug("araqne-logapi-nio: logger [{}] marked deleted file [{}] state", getFullName(), f.getAbsolutePath());
		} else {
			long limitTime = lp.getLastSeen().getTime() + 3600000L;
			if (limitTime <= System.currentTimeMillis()) {
				lastPositions.remove(path);
				slog.debug("araqne-logapi-nio: logger [{}] removed deleted file [{}] from states", getFullName(),
						f.getAbsolutePath());
			}
		}
	}

	protected void processFile(Map<String, LastPosition> lastPositions, File file, CommonHelper helper) {
		if (!file.canRead()) {
			slog.debug("araqne-api-nio: cannot read file [{}], logger [{}]", file.getAbsolutePath(), getFullName());
			return;
		}

		String path = file.getAbsolutePath();
		FileInputStream is = null;
		try {
			String dateFromPath = helper.getDateString(file);

			if (dateFromPath != null && scanPeriodMatcher != null) {
				if (!scanPeriodMatcher.matches(System.currentTimeMillis(), dateFromPath))
					return;
			}

			// skip previous read part
			long offset = 0;
			if (lastPositions.containsKey(path)) {
				LastPosition inform = lastPositions.get(path);
				offset = inform.getPosition();
				slog.trace("araqne-logapi-nio: target file [{}] skip offset [{}]", path, offset);
			}

			if (file.length() <= offset)
				return;

			modifiedStates = true;

			AtomicLong lastPosition = new AtomicLong(offset);
			receiver.filename = file.getName();
			receiver.path = file.getAbsolutePath();
			is = new FileInputStream(file);
			is.skip(offset);

			extractor.extract(is, lastPosition, dateFromPath);

			slog.debug("araqne-logapi-nio: updating file [{}] old position [{}] new last position [{}]",
					new Object[] { path, offset, lastPosition.get() });
			LastPosition inform = lastPositions.get(path);
			if (inform == null) {
				inform = new LastPosition(path);
			}
			inform.setPosition(lastPosition.get());
			lastPositions.put(path, inform);
		} catch (FileNotFoundException e) {
			if (slog.isTraceEnabled())
				slog.trace("araqne-logapi-nio: logger [" + getFullName() + "] read failure: file not found: {}", e.getMessage());
		} catch (Throwable e) {
			slog.error("araqne-logapi-nio: logger [" + getFullName() + "] read error", e);
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
				}
			}
		}
	}

	private class Receiver extends AbstractLogPipe {
		private String filename;
		private String path;

		@Override
		public void onLog(Logger logger, Log log) {
			if (fileTag != null)
				log.getParams().put(fileTag, filename);

			if (pathTag != null)
				log.getParams().put(pathTag, path);

			write(log);
		}

		@Override
		public void onLogBatch(Logger logger, Log[] logs) {
			if (fileTag != null) {
				for (Log log : logs)
					log.getParams().put(fileTag, filename);
			}

			if (pathTag != null) {
				for (Log log : logs)
					log.getParams().put(pathTag, path);
			}

			writeBatch(logs);
		}
	}

	private class InitialRunner implements FileVisitor<Path> {
		private Path root;
		private Map<String, LastPosition> lastPositions;
		private CommonHelper helper;

		public InitialRunner(Path root, Map<String, LastPosition> lastPositions) {
			this.root = root;
			this.lastPositions = lastPositions;
			this.helper = new CommonHelper(dirPathPattern, fileNamePattern, scanPeriodMatcher);
		}

		@Override
		public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
			LoggerStatus status = getStatus();
			if (status == LoggerStatus.Stopping || status == LoggerStatus.Stopped) {
				slog.debug("araqne log api: stop logger [{}] initial runner.", getFullName());
				walkForceStopped = true;
				return FileVisitResult.TERMINATE;
			}

			if (!root.equals(dir) && !recursive) {
				slog.debug("araqne-logapi-nio: logger [{}] skip directory [{}]", getFullName(), dir.toFile().getAbsolutePath());
				return FileVisitResult.SKIP_SUBTREE;
			}

			slog.debug("araqne-logapi-nio: logger [{}] visit directory [{}]", getFullName(), dir.toFile().getAbsolutePath());
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
			LoggerStatus status = getStatus();
			if (status == LoggerStatus.Stopping || status == LoggerStatus.Stopped) {
				slog.debug("araqne log api: stop logger [{}] initial runner.", getFullName());
				walkForceStopped = true;
				return FileVisitResult.TERMINATE;
			}

			slog.debug("araqne-logapi-nio: logger [{}] visit file [{}]", getFullName(), file.toFile().getAbsolutePath());

			File f = file.toFile();
			if (dirPathPattern != null && !dirPathPattern.matcher(f.getParentFile().getAbsolutePath()).find()) {
				slog.debug("araqne-logapi-nio: logger [{}] skip file [{}]", getFullName(), f.getAbsolutePath());
				walkForceStopped = true;
				return FileVisitResult.CONTINUE;
			}

			if (fileNamePattern.matcher(f.getName()).matches())
				processFile(lastPositions, f, helper);
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
			LoggerStatus status = getStatus();
			if (status == LoggerStatus.Stopping || status == LoggerStatus.Stopped) {
				slog.debug("araqne log api: stop logger [{}] initial runner.", getFullName());
				walkForceStopped = true;
				return FileVisitResult.TERMINATE;
			}

			slog.debug("araqne-logapi-nio: logger [{}] visit file failed [{}]", getFullName(), exc.getMessage());
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
			LoggerStatus status = getStatus();
			if (status == LoggerStatus.Stopping || status == LoggerStatus.Stopped) {
				slog.debug("araqne log api: stop logger [{}] initial runner.", getFullName());
				walkForceStopped = true;
				return FileVisitResult.TERMINATE;
			}

			return FileVisitResult.CONTINUE;
		}
	}

	private class ChangeDetector extends Thread implements FileEventListener {
		private FileEventWatcher evtWatcher;
		private Set<File> changedFiles = new HashSet<File>();
		private Set<File> deletedFiles = new HashSet<File>();
		private volatile boolean doStop;
		private volatile boolean deadThread;
		private Object changedLock = new Object();
		private Object deletedLock = new Object();

		public ChangeDetector() {
			super("File Watcher [" + getFullName() + "]");
		}

		public Set<File> getChangedFiles() {
			synchronized (changedLock) {
				Set<File> copied = new HashSet<File>(changedFiles);
				changedFiles.clear();
				return copied;
			}
		}

		public Set<File> getDeletedFiles() {
			synchronized (deletedLock) {
				Set<File> copied = new HashSet<File>(deletedFiles);
				deletedFiles.clear();
				return copied;
			}
		}

		@Override
		public void run() {
			try {
				slog.info("araqne-logapi-nio: starting file watcher for logger [{}]", getFullName());
				this.evtWatcher = new FileEventWatcher(basePath, fileNamePattern, recursive);
				evtWatcher.addListener(detector);

				while (!doStop) {
					evtWatcher.poll(pollTimeout);

					if (pollInterval > 0) {
						try {
							Thread.sleep(pollInterval);
						} catch (InterruptedException e) {
							slog.debug("araqne-logapi-nio: watcher poll sleep interrupted");
						}
					}
				}
			} catch (IOException e) {
				slog.error("araqne-logapi-nio: cannot poll file event for logger [" + getFullName() + "]", e);
			} finally {
				evtWatcher.removeListener(this);
				evtWatcher.close();
				slog.info("araqne-logapi-nio: stopping file watcher for logger [{}]", getFullName());
				deadThread = true;
			}
		}

		public void close() {
			doStop = true;
			slog.debug("araqne-logapi-nio: closing change detector of logger [{}]", getFullName());
		}

		@Override
		public void onCreate(File file) {
			if (file.isFile() && dirPathPattern != null && !dirPathPattern.matcher(file.getParentFile().getAbsolutePath()).find())
				return;

			synchronized (changedLock) {
				changedFiles.add(file);
			}

			if (slog.isDebugEnabled())
				slog.debug("araqne-logapi-nio: logger [{}] detect created file [{}]", getFullName(), file.getAbsolutePath());
		}

		@Override
		public void onDelete(File file) {
			synchronized (changedLock) {
				changedFiles.remove(file);
			}

			synchronized (deletedLock) {
				deletedFiles.add(file);
			}

			if (slog.isDebugEnabled())
				slog.debug("araqne-logapi-nio: logger [{}] detect deleted file [{}]", getFullName(), file.getAbsolutePath());
		}

		@Override
		public void onModify(File file) {
			if (file.isFile() && dirPathPattern != null && !dirPathPattern.matcher(file.getParentFile().getAbsolutePath()).find())
				return;

			synchronized (changedLock) {
				changedFiles.add(file);
			}

			if (slog.isDebugEnabled())
				slog.debug("araqne-logapi-nio: logger [{}] detect modified file [{}]", getFullName(), file.getAbsolutePath());
		}
	}

	@Override
	public void onConfigChange(Map<String, String> oldConfigs, Map<String, String> newConfigs) {
		if (isRunning())
			throw new IllegalStateException("logger is running");

		if (!oldConfigs.get("base_path").equals(newConfigs.get("base_path"))
				|| !oldConfigs.get("filename_pattern").equals(newConfigs.get("filename_pattern"))) {
			setStates(new HashMap<String, Object>());
		}
		applyConfig();
	}
}
