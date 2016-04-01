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
package org.araqne.log.api;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.araqne.log.api.impl.FileUtils;

public class DirectoryWatchLogger extends AbstractLogger implements Reconfigurable {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(DirectoryWatchLogger.class.getName());

	private String basePath;
	private Pattern fileNamePattern;
	private int scanDays;
	private ScanPeriodMatcher scanPeriodMatcher;
	private MultilineLogExtractor extractor;

	private Receiver receiver = new Receiver();

	public DirectoryWatchLogger(LoggerSpecification spec, LoggerFactory factory) {
		super(spec, factory);
		File dataDir = new File(System.getProperty("araqne.data.dir"), "araqne-log-api");
		dataDir.mkdirs();

		// try migration at boot
		File oldLastFile = getLastLogFile(dataDir);
		if (oldLastFile.exists()) {
			Map<String, LastPosition> lastPositions = LastPositionHelper.readLastPositions(oldLastFile);
			setStates(LastPositionHelper.serialize(lastPositions));
			oldLastFile.renameTo(new File(oldLastFile.getAbsolutePath() + ".migrated"));
		}

		applyConfig();
	}

	@Override
	public void onConfigChange(Map<String, String> oldConfigs, Map<String, String> newConfigs) {
		if (!oldConfigs.get("base_path").equals(newConfigs.get("base_path"))
				|| !oldConfigs.get("filename_pattern").equals(newConfigs.get("filename_pattern"))) {
			setStates(new HashMap<String, Object>());
		}

		applyConfig();
	}

	private void applyConfig() {
		Map<String, String> configs = getConfigs();

		this.basePath = configs.get("base_path");
		this.fileNamePattern = Pattern.compile(configs.get("filename_pattern"));

		receiver.fileTag = configs.get("file_tag");
		receiver.pathTag = configs.get("path_tag");

		this.extractor = MultilineLogExtractor.build(this, receiver);

		// optional
		String scanDaysString = getConfigs().get("scan_days");
		if (scanDaysString != null) {
			try {
				this.scanDays = Integer.parseInt(scanDaysString);
				if (scanDays < 0)
					slog.warn(
							"araqne log api: logger [" + getFullName() + "] has invalid scan days [{}], config will be ignored.",
							scanDaysString);
			} catch (NumberFormatException e) {
				slog.warn("araqne log api: logger [" + getFullName() + "] has invalid scan days [{}], config will be ignored.",
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
				slog.warn("araqne log api: logger [" + getFullName() + "] has invalid path date format [" + pathDateFormatString
						+ "], locale [" + extractor.getDateLocale() + "], timezone ["
						+ extractor.getDateFormat().getTimeZone().getDisplayName() + "]", t);
			}
		}
	}

	@Override
	protected void runOnce() {
		List<File> logFiles = FileUtils.matches(basePath, fileNamePattern);
		Map<String, LastPosition> lastPositions = LastPositionHelper.deserialize(getStates());
		CommonHelper helper = new CommonHelper(fileNamePattern, scanPeriodMatcher);

		try {
			for (File f : logFiles) {
				processFile(lastPositions, f, helper);
			}
		} finally {
			helper.removeOutdatedStates(lastPositions);
			lastPositions = updateLastSeen(lastPositions, logFiles);
			setStates(LastPositionHelper.serialize(lastPositions));
		}
	}

	private Map<String, LastPosition> updateLastSeen(Map<String, LastPosition> lastPositions, List<File> logFiles) {
		Map<String, LastPosition> updatedLastPositions = new HashMap<String, LastPosition>();
		Set<String> filePaths = new HashSet<String>();
		for (File f : logFiles)
			filePaths.add(f.getAbsolutePath());

		long currentTime = new Date().getTime();
		for (String path : lastPositions.keySet()) {
			LastPosition p = lastPositions.get(path);
			if (p.getLastSeen() != null) {
				long limitTime = p.getLastSeen().getTime() + 3600000L;
				if (filePaths.contains(path)) {
					p.setLastSeen(null);
				} else if (limitTime <= currentTime) {
					continue;
				}
			} else {
				if (!filePaths.contains(path)) {
					p.setLastSeen(new Date());
				}
			}

			updatedLastPositions.put(path, p);
		}
		return updatedLastPositions;
	}

	protected void processFile(Map<String, LastPosition> lastPositions, File f, CommonHelper helper) {
		if (!f.canRead()) {
			slog.debug("araqne log api: cannot read file [{}], logger [{}]", f.getAbsolutePath(), getFullName());
			return;
		}

		String dateFromPath = helper.getDateString(f);
		if (dateFromPath != null && scanPeriodMatcher != null) {
			if (!scanPeriodMatcher.matches(System.currentTimeMillis(), dateFromPath))
				return;
		}

		FileInputStream is = null;
		String path = f.getAbsolutePath();
		try {
			// get date pattern-matched string from filename
			String dateFromFileName = helper.getDateString(f);

			// skip previous read part
			long offset = 0;
			if (lastPositions.containsKey(path)) {
				LastPosition inform = lastPositions.get(path);
				offset = inform.getPosition();
				slog.trace("araqne log api: target file [{}] skip offset [{}]", path, offset);
			}

			AtomicLong lastPosition = new AtomicLong(offset);
			File file = new File(path);
			if (file.length() <= offset)
				return;

			is = new FileInputStream(file);
			is.skip(offset);

			receiver.fileName = file.getName();
			receiver.filePath = file.getAbsolutePath();
			extractor.extract(is, lastPosition, dateFromFileName);

			slog.debug("araqne log api: updating file [{}] old position [{}] new last position [{}]",
					new Object[] { path, offset, lastPosition.get() });
			LastPosition inform = lastPositions.get(path);
			if (inform == null) {
				inform = new LastPosition(path);
			}
			inform.setPosition(lastPosition.get());
			lastPositions.put(path, inform);
		} catch (FileNotFoundException e) {
			if (slog.isTraceEnabled())
				slog.trace("araqne log api: [" + getName() + "] logger read failure: file not found: {}", e.getMessage());
		} catch (Throwable e) {
			slog.error("araqne log api: [" + getName() + "] logger read error", e);
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
				}
			}
		}
	}

	protected File getLastLogFile(File dataDir) {
		return new File(dataDir, "dirwatch-" + getName() + ".lastlog");
	}

	private class Receiver extends AbstractLogPipe {
		private String fileTag;
		private String fileName;
		private String pathTag;
		private String filePath;

		@Override
		public void onLog(Logger logger, Log log) {
			if (fileTag != null)
				log.getParams().put(fileTag, fileName);

			if (pathTag != null)
				log.getParams().put(pathTag, filePath);
			write(log);
		}

		@Override
		public void onLogBatch(Logger logger, Log[] logs) {
			if (fileTag != null) {
				for (Log log : logs) {
					log.getParams().put(fileTag, fileName);
				}
			}

			if (pathTag != null) {
				for (Log log : logs) {
					log.getParams().put(pathTag, filePath);
				}
			}
			writeBatch(logs);
		}

	}
}
