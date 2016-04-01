package org.araqne.log.api.nio;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
import org.araqne.log.api.MultilineLogExtractor;
import org.araqne.log.api.Reconfigurable;
import org.araqne.log.api.ScanPeriodMatcher;

public class NaiveRecursiveDirectoryWatchLogger extends AbstractLogger implements Reconfigurable {
	private final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NaiveRecursiveDirectoryWatchLogger.class);
	private String basePath;
	private Pattern fileNamePattern;
	private Pattern dirPathPattern;
	private boolean recursive;
	private String fileTag;
	private String pathTag;
	private int scanDays;
	private MultilineLogExtractor extractor;
	private ScanPeriodMatcher scanPeriodMatcher;

	private Receiver receiver = new Receiver();

	public NaiveRecursiveDirectoryWatchLogger(LoggerSpecification spec, LoggerFactory factory) {
		super(spec, factory);

		if (logger.isDebugEnabled())
			logger.debug("araqne-logapi-nio: recursive dirwatcher used recursive scan");

		applyConfig();
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
					logger.warn("araqne logapi nio: logger [" + getFullName()
							+ "] has invalid scan days [{}], config will be ignored.", scanDaysString);
			} catch (NumberFormatException e) {
				logger.warn(
						"araqne logapi nio: logger [" + getFullName() + "] has invalid scan days [{}], config will be ignored.",
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
				logger.warn("araqne logapi nio: logger [" + getFullName() + "] has invalid path date format ["
						+ pathDateFormatString + "], locale [" + extractor.getDateLocale() + "], timezone ["
						+ extractor.getDateFormat().getTimeZone().getDisplayName() + "]", t);
			}
		}
	}

	@Override
	protected void runOnce() {
		Map<String, LastPosition> lastPositions = LastPositionHelper.deserialize(getStates());
		List<File> files = getFiles();
		if (files == null)
			return;

		CommonHelper helper = new CommonHelper(dirPathPattern, fileNamePattern, scanPeriodMatcher);
		Collections.sort(files);
		try {
			for (File f : files)
				processFile(f, lastPositions, helper);
		} finally {
			helper.removeOutdatedStates(lastPositions);
			setStates(LastPositionHelper.serialize(lastPositions));
		}
	}

	private void processFile(File f, Map<String, LastPosition> lastPositions, CommonHelper helper) {
		FileInputStream is = null;

		try {
			String dateFromPath = helper.getDateString(f);
			if (dateFromPath != null && scanPeriodMatcher != null) {
				if (!scanPeriodMatcher.matches(System.currentTimeMillis(), dateFromPath))
					return;
			}

			String path = f.getAbsolutePath();
			long offset = 0;
			if (lastPositions.containsKey(path)) {
				LastPosition inform = lastPositions.get(path);
				offset = inform.getPosition();
				logger.trace("araqne-logapi-nio: target file [{}] skip offset [{}]", path, offset);
			}

			AtomicLong lastPosition = new AtomicLong(offset);
			File file = new File(path);
			if (file.length() <= offset)
				return;

			receiver.filename = file.getName();
			receiver.path = file.getAbsolutePath();
			is = new FileInputStream(file);
			is.skip(offset);

			extractor.extract(is, lastPosition, dateFromPath);

			logger.debug("araqne-logapi-nio: updating file [{}] old position [{}] new last position [{}]",
					new Object[] { path, offset, lastPosition.get() });
			LastPosition inform = lastPositions.get(path);
			if (inform == null) {
				inform = new LastPosition(path);
			}
			inform.setPosition(lastPosition.get());
			lastPositions.put(path, inform);
		} catch (Throwable e) {
			logger.error("araqne-logapi-nio: [" + getName() + "] logger read error", e);
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
				}
			}
		}
	}

	private List<File> getFiles() {
		File base = new File(basePath);
		if (!base.exists() || !base.isDirectory()) {
			logger.debug("araqne logapi nio: invalid base path [{}]", basePath);
			return null;
		}

		List<File> targetFiles = new ArrayList<File>();
		for (File f : base.listFiles()) {
			if (f.isDirectory())
				targetFiles.addAll(getFiles(f));
			else if (dirPathPattern == null || dirPathPattern.matcher(base.getAbsolutePath()).find())
				targetFiles.addAll(getFiles(f));
		}
		return targetFiles;
	}

	private List<File> getFiles(File f) {
		List<File> files = new ArrayList<File>();
		if (f.isDirectory() && recursive) {
			if (dirPathPattern == null || dirPathPattern.matcher(f.getAbsolutePath()).find()) {
				for (File subFile : f.listFiles())
					files.addAll(getFiles(subFile));
			}
		} else {
			if (fileNamePattern.matcher(f.getName()).matches())
				files.add(f);
		}

		return files;
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
				for (Log log : logs) {
					log.getParams().put(fileTag, filename);
				}
			}

			if (pathTag != null) {
				for (Log log : logs)
					log.getParams().put(pathTag, path);
			}

			writeBatch(logs);
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
