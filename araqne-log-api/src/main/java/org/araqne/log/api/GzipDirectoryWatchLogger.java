package org.araqne.log.api;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public class GzipDirectoryWatchLogger extends AbstractLogger {
	private final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GzipDirectoryWatchLogger.class);

	private String basePath;
	private Pattern fileNamePattern;
	private boolean isDeleteFile = false;

	public GzipDirectoryWatchLogger(LoggerSpecification spec, LoggerFactory factory) {
		super(spec, factory);

		basePath = getConfigs().get("base_path");

		String fileNameRegex = getConfigs().get("filename_pattern");
		fileNamePattern = Pattern.compile(fileNameRegex);

		// optional
		if (getConfigs().containsKey("is_delete"))
			isDeleteFile = Boolean.parseBoolean(getConfigs().get("is_delete"));
	}

	private MultilineLogExtractor newExtractor(Receiver receiver, long offset) {
		MultilineLogExtractor extractor = new MultilineLogExtractor(this, receiver);

		// optional
		String dateExtractRegex = getConfigs().get("date_pattern");
		if (dateExtractRegex != null)
			extractor.setDateMatcher(Pattern.compile(dateExtractRegex).matcher(""));

		// optional
		String dateLocale = getConfigs().get("date_locale");
		if (dateLocale == null)
			dateLocale = "en";

		// optional
		String dateFormatString = getConfigs().get("date_format");
		if (dateFormatString != null)
			extractor.setDateFormat(new SimpleDateFormat(dateFormatString, new Locale(dateLocale)));

		// optional
		String newlogRegex = getConfigs().get("newlog_designator");
		if (newlogRegex != null)
			extractor.setBeginMatcher(Pattern.compile(newlogRegex).matcher(""));

		String newlogEndRegex = getConfigs().get("newlog_end_designator");
		if (newlogEndRegex != null)
			extractor.setEndMatcher(Pattern.compile(newlogEndRegex).matcher(""));

		// optional
		String charset = getConfigs().get("charset");
		if (charset == null)
			charset = "utf-8";

		extractor.setCharset(charset);

		return extractor;
	}

	@Override
	protected void runOnce() {
		List<String> logFiles = FileUtils.matchFiles(basePath, fileNamePattern);
		Map<String, LastPosition> lastPositions = LastPositionHelper.deserialize(getStates());

		try {
			for (String path : logFiles) {
				LoggerStatus status = getStatus();
				if (status == LoggerStatus.Stopping || status == LoggerStatus.Stopped)
					break;

				processFile(lastPositions, path);
			}
		} finally {
			setStates(LastPositionHelper.serialize(lastPositions));
		}
	}

	protected void processFile(Map<String, LastPosition> lastPositions, String path) {
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
			MultilineLogExtractor extractor = newExtractor(receiver, offset);

			File file = new File(path);
			fis = new FileInputStream(file);
			gis = new GZIPInputStream(fis);

			String dateFromFileName = getDateFromFileName(path);
			extractor.extract(gis, new AtomicLong(), dateFromFileName);
			position.setPosition(-1);
		} catch (FileNotFoundException e) {
			logger.trace("araqne log api: [{}] logger read failure: file not found: {}", getName(), e.getMessage());
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

	private String getDateFromFileName(String path) {
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
