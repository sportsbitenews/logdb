package org.araqne.log.api.nio;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.araqne.log.api.AbstractLogPipe;
import org.araqne.log.api.AbstractLogger;
import org.araqne.log.api.LastPosition;
import org.araqne.log.api.LastPositionHelper;
import org.araqne.log.api.Log;
import org.araqne.log.api.Logger;
import org.araqne.log.api.LoggerFactory;
import org.araqne.log.api.LoggerSpecification;
import org.araqne.log.api.MultilineLogExtractor;
import org.araqne.log.api.Reconfigurable;

public class NaiveRecursiveDirectoryWatchLogger extends AbstractLogger implements Reconfigurable {
	private final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(NaiveRecursiveDirectoryWatchLogger.class);
	private String basePath;
	private Pattern fileNamePattern;
	private Pattern dirPathPattern;
	private boolean recursive;

	public NaiveRecursiveDirectoryWatchLogger(LoggerSpecification spec, LoggerFactory factory) {
		super(spec, factory);

		if (logger.isDebugEnabled())
			logger.debug("araqne-logapi-nio: recursive dirwatcher used recursive scan");

		load();
	}

	@Override
	public void onConfigChange(Map<String, String> oldConfigs, Map<String, String> newConfigs) {
		load();
		if (!oldConfigs.get("base_path").equals(newConfigs.get("base_path"))
				|| !oldConfigs.get("filename_pattern").equals(newConfigs.get("filename_pattern"))) {
			setStates(new HashMap<String, Object>());
		}
	}

	private void load() {
		basePath = getConfigs().get("base_path");
		String fileNameRegex = getConfigs().get("filename_pattern");
		fileNamePattern = Pattern.compile(fileNameRegex);

		String dirNameRegex = getConfigs().get("dirpath_pattern");
		if (dirNameRegex != null)
			dirPathPattern = Pattern.compile(dirNameRegex);

		String recursiveStr = getConfigs().get("recursive");
		recursive = ((recursiveStr != null) && (recursiveStr.compareToIgnoreCase("true") == 0));
	}

	@Override
	protected void runOnce() {
		Map<String, LastPosition> lastPositions = LastPositionHelper.deserialize(getStates());
		List<File> files = getFiles();
		if (files == null)
			return;

		Collections.sort(files);
		try {
			for (File f : files)
				processFile(f, lastPositions);
		} finally {
			setStates(LastPositionHelper.serialize(lastPositions));
		}
	}

	private void processFile(File f, Map<String, LastPosition> lastPositions) {
		FileInputStream is = null;

		try {
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

			is = new FileInputStream(file);
			is.skip(offset);

			Receiver receiver = new Receiver(getConfigs().get("file_tag"), file.getName());
			MultilineLogExtractor extractor = MultilineLogExtractor.build(this, receiver);
			extractor.extract(is, lastPosition, getDateString(f));

			logger.debug("araqne-logapi-nio: updating file [{}] old position [{}] new last position [{}]", new Object[] { path,
					offset, lastPosition.get() });
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

	private String getDateString(File f) {
		StringBuffer sb = new StringBuffer(f.getAbsolutePath().length());
		String dirPath = f.getParentFile().getAbsolutePath();
		if (dirPathPattern != null) {
			Matcher dirNameDateMatcher = dirPathPattern.matcher(dirPath);
			while (dirNameDateMatcher.find()) {
				int dirNameGroupCount = dirNameDateMatcher.groupCount();
				if (dirNameGroupCount > 0) {
					for (int i = 1; i <= dirNameGroupCount; ++i) {
						sb.append(dirNameDateMatcher.group(i));
					}
				}
			}
		}

		String fileName = f.getName();
		Matcher fileNameDateMatcher = fileNamePattern.matcher(fileName);
		while (fileNameDateMatcher.find()) {
			int fileNameGroupCount = fileNameDateMatcher.groupCount();
			if (fileNameGroupCount > 0) {
				for (int i = 1; i <= fileNameGroupCount; ++i) {
					sb.append(fileNameDateMatcher.group(i));
				}
			}
		}
		String date = sb.toString();
		return date.isEmpty() ? null : date;
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
		private String fileTag;
		private String fileName;

		public Receiver(String fileTag, String fileName) {
			this.fileTag = fileTag;
			this.fileName = fileName;
		}

		@Override
		public void onLog(Logger logger, Log log) {
			if (fileTag != null)
				log.getParams().put(fileTag, fileName);
			write(log);
		}

		@Override
		public void onLogBatch(Logger logger, Log[] logs) {
			if (fileTag != null) {
				for (Log log : logs) {
					log.getParams().put(fileTag, fileName);
				}
			}
			writeBatch(logs);
		}
	}
}
