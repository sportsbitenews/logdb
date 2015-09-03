package org.araqne.log.api;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public class GzipDirectoryWatchLogger extends AbstractLogger implements Reconfigurable {
	private final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GzipDirectoryWatchLogger.class);

	public GzipDirectoryWatchLogger(LoggerSpecification spec, LoggerFactory factory) {
		super(spec, factory);
	}

	@Override
	public void onConfigChange(Map<String, String> oldConfigs, Map<String, String> newConfigs) {
		if (!oldConfigs.get("base_path").equals(newConfigs.get("base_path"))) {
			setStates(new HashMap<String, Object>());
		}
	}

	@Override
	protected void runOnce() {
		Map<String, String> configs = getConfigs();

		String basePath = configs.get("base_path");
		Pattern fileNamePattern = Pattern.compile(configs.get("filename_pattern"));

		boolean isDeleteFile = false;
		if (configs.containsKey("is_delete"))
			isDeleteFile = Boolean.parseBoolean(getConfigs().get("is_delete"));

		List<String> logFiles = FileUtils.matchFiles(basePath, fileNamePattern);
		Map<String, LastPosition> lastPositions = LastPositionHelper.deserialize(getStates());
		removeStatesOfDeletedFiles(lastPositions, logFiles);

		try {
			for (String path : logFiles) {
				LoggerStatus status = getStatus();
				if (status == LoggerStatus.Stopping || status == LoggerStatus.Stopped)
					break;

				processFile(lastPositions, path, isDeleteFile, fileNamePattern);
			}
		} finally {
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

	protected void processFile(Map<String, LastPosition> lastPositions, String path, boolean isDeleteFile, Pattern fileNamePattern) {
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

			Receiver receiver = new Receiver(position);
			MultilineLogExtractor extractor = MultilineLogExtractor.build(this, receiver);

			File file = new File(path);
			fis = new FileInputStream(file);
			gis = new GZIPInputStream(fis);

			String dateFromFileName = getDateFromFileName(path, fileNamePattern);
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

	private String getDateFromFileName(String path, Pattern fileNamePattern) {
		String dateFromFileName = null;
		Matcher fileNameDateMatcher = fileNamePattern.matcher(path);
		if (fileNameDateMatcher.find()) {
			int fileNameGroupCount = fileNameDateMatcher.groupCount();
			if (fileNameGroupCount > 0) {
				StringBuffer sb = new StringBuffer();
				for (int i = 1; i <= fileNameGroupCount; ++i) {
					sb.append(fileNameDateMatcher.group(i));
				}
				dateFromFileName = sb.toString();
			}
		}
		return dateFromFileName;
	}

	private class Receiver extends AbstractLogPipe {
		private long offset;
		private LastPosition position;

		public Receiver(LastPosition position) {
			this.position = position;
		}

		@Override
		public void onLog(Logger logger, Log log) {
			if (position.getPosition() < offset)
				write(log);

			position.setPosition(position.getPosition() + 1);
		}

		@Override
		public void onLogBatch(Logger logger, Log[] logs) {
			List<Log> filtered = new ArrayList<Log>();

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
