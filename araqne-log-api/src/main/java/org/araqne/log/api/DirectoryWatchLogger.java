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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.araqne.log.api.impl.FileUtils;

public class DirectoryWatchLogger extends AbstractLogger {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(DirectoryWatchLogger.class.getName());
	protected File dataDir;
	protected String basePath;
	protected Pattern fileNamePattern;
	private Receiver receiver = new Receiver();

	private MultilineLogExtractor extractor;

	public DirectoryWatchLogger(LoggerSpecification spec, LoggerFactory factory) {
		super(spec, factory);

		dataDir = new File(System.getProperty("araqne.data.dir"), "araqne-log-api");
		dataDir.mkdirs();
		basePath = getConfigs().get("base_path");

		String fileNameRegex = getConfigs().get("filename_pattern");
		fileNamePattern = Pattern.compile(fileNameRegex);

		extractor = new MultilineLogExtractor(this, receiver);

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
		String timeZone = getConfigs().get("timezone");
		if (dateFormatString != null)
			extractor.setDateFormat(new SimpleDateFormat(dateFormatString, new Locale(dateLocale)), timeZone);

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

		// try migration at boot
		File oldLastFile = getLastLogFile();
		if (oldLastFile.exists()) {
			Map<String, LastPosition> lastPositions = LastPositionHelper.readLastPositions(oldLastFile);
			setStates(LastPositionHelper.serialize(lastPositions));
			oldLastFile.renameTo(new File(oldLastFile.getAbsolutePath() + ".migrated"));
		}
	}

	@Override
	protected void runOnce() {
		List<File> logFiles = FileUtils.matches(basePath, fileNamePattern);
		Map<String, LastPosition> lastPositions = LastPositionHelper.deserialize(getStates());

		for (File f : logFiles) {
			processFile(lastPositions, f);
		}

		lastPositions = updateLastSeen(lastPositions, logFiles);
		setStates(LastPositionHelper.serialize(lastPositions));
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

	protected void processFile(Map<String, LastPosition> lastPositions, File f) {
		if (!f.canRead()) {
			slog.debug("araqne log api: cannot read file [{}], logger [{}]", f.getAbsolutePath(), getFullName());
			return;
		}

		FileInputStream is = null;
		String path = f.getAbsolutePath();
		try {
			// get date pattern-matched string from filename
			String dateFromFileName = null;
			Matcher fileNameDateMatcher = fileNamePattern.matcher(f.getName());
			while (fileNameDateMatcher.find()) {
				int fileNameGroupCount = fileNameDateMatcher.groupCount();
				if (fileNameGroupCount > 0) {
					StringBuffer sb = new StringBuffer();
					for (int i = 1; i <= fileNameGroupCount; ++i) {
						sb.append(fileNameDateMatcher.group(i));
					}
					dateFromFileName = sb.toString();
				}
			}

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

			extractor.extract(is, lastPosition, dateFromFileName);

			slog.debug("araqne log api: updating file [{}] old position [{}] new last position [{}]", new Object[] { path,
					offset, lastPosition.get() });
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

	protected File getLastLogFile() {
		return new File(dataDir, "dirwatch-" + getName() + ".lastlog");
	}

	private class Receiver extends AbstractLogPipe {

		@Override
		public void onLog(Logger logger, Log log) {
			write(log);
		}

		@Override
		public void onLogBatch(Logger logger, Log[] logs) {
			writeBatch(logs);
		}

	}
}
