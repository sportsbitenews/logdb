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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

public class RotationFileLogger extends AbstractLogger implements LogPipe {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(RotationFileLogger.class);
	private final File dataDir;

	private String charset;

	private MultilineLogExtractor extractor;

	public RotationFileLogger(LoggerSpecification spec, LoggerFactory factory) {
		super(spec, factory);

		this.dataDir = new File(System.getProperty("araqne.data.dir"), "araqne-log-api");
		this.dataDir.mkdirs();

		extractor = new MultilineLogExtractor(this, this);

		// optional
		String datePatternRegex = getConfig().get("date_pattern");
		if (datePatternRegex != null) {
			extractor.setDateMatcher(Pattern.compile(datePatternRegex).matcher(""));
		}

		// optional
		String dateLocale = getConfig().get("date_locale");
		if (dateLocale == null)
			dateLocale = "en";

		// optional
		String dateFormatString = getConfig().get("date_format");
		if (dateFormatString != null)
			extractor.setDateFormat(new SimpleDateFormat(dateFormatString, new Locale(dateLocale)));

		// optional
		String beginRegex = getConfig().get("begin_regex");
		if (beginRegex != null)
			extractor.setBeginMatcher(Pattern.compile(beginRegex).matcher(""));

		String endRegex = getConfig().get("end_regex");
		if (endRegex != null)
			extractor.setEndMatcher(Pattern.compile(endRegex).matcher(""));

		// optional
		charset = getConfig().get("charset");
		if (charset == null)
			charset = "utf-8";

		extractor.setCharset(charset);

	}

	@Override
	public void onLog(Logger logger, Log log) {
		write(log);
	}

	@Override
	protected void runOnce() {
		LastState state = getLastPosition();

		String filePath = getConfig().get("file_path");
		File f = new File(filePath);
		if (!f.exists()) {
			slog.debug("araqne log api: rotation logger [{}] file [{}] not found", getFullName(), filePath);
			return;
		}

		if (!f.canRead()) {
			slog.debug("araqne log api: rotation logger [{}] file [{}] no read permission", getFullName(), filePath);
			return;
		}

		String firstLine = readFirstLine(f);
		long fileLength = f.length();
		long offset = 0;

		if (state != null) {
			if (firstLine == null || !firstLine.equals(state.firstLine) || fileLength < state.lastLength)
				offset = 0;
			else
				offset = state.lastPosition;
		}

		AtomicLong lastPosition = new AtomicLong(offset);

		FileInputStream is = null;

		try {
			is = new FileInputStream(f);
			is.skip(offset);
			extractor.extract(is, lastPosition);
		} catch (Throwable t) {
			slog.error("araqne log api: cannot read file", t);
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
				}
			}

			updateLastState(getLastLogFile(), fileLength, lastPosition.get(), firstLine);
		}
	}

	private void updateLastState(File f, long length, long pos, String firstLine) {
		// write last positions
		FileOutputStream os = null;

		try {
			os = new FileOutputStream(f);

			String line = length + "\n" + pos + "\n" + firstLine + "\n";
			os.write(line.getBytes("utf-8"));
		} catch (IOException e) {
			slog.error("araqne log api: cannot write last position file", e);
		} finally {
			if (os != null) {
				try {
					os.close();
				} catch (IOException e) {
				}
			}
		}
	}

	private String readFirstLine(File f) {
		FileInputStream is = null;
		BufferedReader br = null;

		try {
			is = new FileInputStream(f);
			br = new BufferedReader(new InputStreamReader(is, charset));
			return br.readLine();
		} catch (Throwable t) {
			slog.error("araqne log api: cannot read first line, logger [" + getFullName() + "]", t);
			return null;
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
				}
			}

			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
				}
			}
		}
	}

	private LastState getLastPosition() {
		FileInputStream is = null;
		BufferedReader br = null;
		try {
			File f = getLastLogFile();
			if (!f.exists() || !f.canRead())
				return null;

			is = new FileInputStream(f);
			br = new BufferedReader(new InputStreamReader(is, "utf-8"));
			long len = Long.valueOf(br.readLine());
			long pos = Long.valueOf(br.readLine());
			String line = br.readLine();
			return new LastState(line, pos, len);
		} catch (Throwable t) {
			slog.error("araqne log api: cannot read last position", t);
			return null;
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
				}
			}
		}
	}

	protected File getLastLogFile() {
		return new File(dataDir, "rotation-" + getName() + ".lastlog");
	}

	private class LastState {
		private String firstLine;
		private long lastPosition;
		private long lastLength;

		public LastState(String firstLine, long lastPosition, long lastLength) {
			this.firstLine = firstLine;
			this.lastPosition = lastPosition;
			this.lastLength = lastLength;
		}
	}
}
