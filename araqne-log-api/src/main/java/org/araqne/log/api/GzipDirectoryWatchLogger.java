package org.araqne.log.api;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public class GzipDirectoryWatchLogger extends AbstractLogger implements Reconfigurable {
	private final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GzipDirectoryWatchLogger.class);

	private String basePath;
	private Pattern fileNamePattern;
	private int scanDays;
	private ScanPeriodMatcher scanPeriodMatcher;
	private MultilineLogExtractor extractor;
	private boolean isDeleteFile;

	private Receiver receiver = new Receiver();

	public GzipDirectoryWatchLogger(LoggerSpecification spec, LoggerFactory factory) {
		super(spec, factory);

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

		basePath = configs.get("base_path");
		fileNamePattern = Pattern.compile(configs.get("filename_pattern"));

		isDeleteFile = false;
		if (configs.containsKey("is_delete"))
			isDeleteFile = Boolean.parseBoolean(getConfigs().get("is_delete"));

		receiver.fileTag = configs.get("file_tag");
		receiver.pathTag = configs.get("path_tag");

		this.extractor = MultilineLogExtractor.build(this, receiver);

		// optional
		String scanDaysString = getConfigs().get("scan_days");
		if (scanDaysString != null) {
			try {
				this.scanDays = Integer.parseInt(scanDaysString);
				if (scanDays < 0)
					logger.warn(
							"araqne log api: logger [" + getFullName() + "] has invalid scan days [{}], config will be ignored.",
							scanDaysString);
			} catch (NumberFormatException e) {
				logger.warn("araqne log api: logger [" + getFullName() + "] has invalid scan days [{}], config will be ignored.",
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
				logger.warn("araqne log api: logger [" + getFullName() + "] has invalid path date format [" + pathDateFormatString
						+ "], locale [" + extractor.getDateLocale() + "], timezone ["
						+ extractor.getDateFormat().getTimeZone().getDisplayName() + "]", t);
			}
		}
	}

	@Override
	protected void runOnce() {
		List<String> logFiles = FileUtils.matchFiles(basePath, fileNamePattern);
		Map<String, LastPosition> lastPositions = LastPositionHelper.deserialize(getStates());
		removeStatesOfDeletedFiles(lastPositions, logFiles);
		CommonHelper helper = new CommonHelper(fileNamePattern, scanPeriodMatcher);

		try {
			for (String path : logFiles) {
				LoggerStatus status = getStatus();
				if (status == LoggerStatus.Stopping || status == LoggerStatus.Stopped)
					break;

				processFile(lastPositions, path, helper);
			}
		} finally {
			helper.removeOutdatedStates(lastPositions);
			setStates(LastPositionHelper.serialize(lastPositions));
		}
	}

	private void removeStatesOfDeletedFiles(Map<String, LastPosition> lastPositions, List<String> currentFiles) {
		Set<String> currentSet = new HashSet<String>(currentFiles);
		for (String path : new ArrayList<String>(lastPositions.keySet())) {
			if (!currentSet.contains(path))
				lastPositions.remove(path);
		}
	}

	protected void processFile(Map<String, LastPosition> lastPositions, String path, CommonHelper helper) {
		LastPosition position = new LastPosition(path);
		FileInputStream fis = null;
		GZIPInputStream gis = null;

		try {
			// skip previous read part
			long offset = 0;
			LastPosition oldPosition = lastPositions.get(path);
			if (oldPosition != null) {
				offset = oldPosition.getPosition();
				position.setPosition(offset);
				position.setLastSeen(oldPosition.getLastSeen());
				logger.trace("araqne log api: target gzip file [{}] skip offset [{}]", path, offset);
			}

			if (offset == -1)
				return;

			File file = new File(path);
			fis = new FileInputStream(file);
			gis = new GZIPInputStream(fis);

			receiver.fileName = file.getName();
			receiver.filePath = file.getAbsolutePath();

			String dateFromFileName = helper.getDateString(file);
			extractor.extract(gis, new AtomicLong(), dateFromFileName);
			position.setPosition(-1);
		} catch (EOFException e) {
			// abnormal end of content (e.g. missing new line), but read
			// completed successfully
			position.setPosition(-1);
		} catch (FileNotFoundException e) {
			logger.trace("araqne log api: [{}] logger read failure: file not found: {}", getName(), e.getMessage());
			position.setPosition(-1);
		} catch (Throwable e) {
			logger.error("araqne log api: [" + getName() + "] logger read error", e);

		} finally {
			FileUtils.ensureClose(gis);
			FileUtils.ensureClose(fis);
			lastPositions.put(path, position);

			if (isDeleteFile && position.getPosition() == -1 && new File(path).delete()) {
				lastPositions.remove(path);
				logger.trace("araqne log api: deleted gzip file [{}]", path);
			}
		}
	}

	private class Receiver extends AbstractLogPipe {
		private long offset;
		private LastPosition position;
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

			if (position.getPosition() < offset)
				write(log);

			position.setPosition(position.getPosition() + 1);
		}

		@Override
		public void onLogBatch(Logger logger, Log[] logs) {
			List<Log> filtered = new ArrayList<Log>();

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

			for (Log log : logs) {
				if (log != null) {
					if (position.getPosition() >= offset)
						filtered.add(log);

					position.setPosition(position.getPosition() + 1);
				}
			}

			writeBatch(filtered.toArray(new Log[0]));
		}
	}
}
