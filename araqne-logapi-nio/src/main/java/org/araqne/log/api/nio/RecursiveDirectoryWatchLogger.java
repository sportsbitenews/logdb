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
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
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

public class RecursiveDirectoryWatchLogger extends AbstractLogger {

	private final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RecursiveDirectoryWatchLogger.class.getName());
	protected String basePath;
	protected Pattern fileNamePattern;
	private Receiver receiver = new Receiver();
	private boolean recursive;
	private String fileTag;

	private MultilineLogExtractor extractor;

	public RecursiveDirectoryWatchLogger(LoggerSpecification spec, LoggerFactory factory) {
		super(spec, factory);

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

		// optional
		String recursive = getConfigs().get("recursive");
		this.recursive = ((recursive != null) && (recursive.compareToIgnoreCase("true") == 0));

		// optional
		this.fileTag = getConfigs().get("file_tag");

		extractor.setCharset(charset);

	}

	@Override
	protected void runOnce() {
		Map<String, LastPosition> lastPositions = LastPositionHelper.deserialize(getStates());

		setStates(LastPositionHelper.serialize(lastPositions));
	}

	protected void processFile(Map<String, LastPosition> lastPositions, File file) {
		String path = file.getAbsolutePath();
		FileInputStream is = null;
		try {
			// get date pattern-matched string from filename
			String dateFromFileName = null;
			Matcher fileNameDateMatcher = fileNamePattern.matcher(path);
			if (fileNameDateMatcher.find()) {
				int fileNameGroupCount = fileNameDateMatcher.groupCount();
				if (fileNameGroupCount > 0) {
					StringBuilder sb = new StringBuilder();
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
				logger.trace("araqne-logapi-nio: target file [{}] skip offset [{}]", path, offset);
			}

			AtomicLong lastPosition = new AtomicLong(offset);
			if (file.length() <= offset)
				return;

			receiver.filename = file.getName();
			is = new FileInputStream(file);
			is.skip(offset);

			extractor.extract(is, lastPosition, dateFromFileName);

			logger.debug("araqne-logapi-nio: updating file [{}] old position [{}] new last position [{}]", new Object[] { path,
					offset, lastPosition.get() });
			LastPosition inform = lastPositions.get(path);
			if (inform == null) {
				inform = new LastPosition(path);
			}
			inform.setPosition(lastPosition.get());
			lastPositions.put(path, inform);
		} catch (FileNotFoundException e) {
			if (logger.isTraceEnabled())
				logger.trace("araqne-logapi-nio: logger [" + getFullName() + "] read failure: file not found: {}", e.getMessage());
		} catch (Throwable e) {
			logger.error("araqne-logapi-nio: logger [" + getFullName() + "] read error", e);
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

		@Override
		public void onLog(Logger logger, Log log) {
			if (fileTag != null)
				log.getParams().put(fileTag, filename);
			write(log);
		}

		@Override
		public void onLogBatch(Logger logger, Log[] logs) {
			if (fileTag != null) {
				for (Log log : logs) {
					log.getParams().put(fileTag, filename);
				}
			}
			writeBatch(logs);
		}
	}

	private class ChangeDetector implements FileEventListener {

		private Set<File> changedFiles = new HashSet<File>();
		private Set<File> deletedFiles = new HashSet<File>();

		@Override
		public void onCreate(File file) {
			changedFiles.add(file);
		}

		@Override
		public void onDelete(File file) {
			changedFiles.remove(file);
			deletedFiles.add(file);
		}

		@Override
		public void onModify(File file) {
			changedFiles.add(file);
		}
	}
}
