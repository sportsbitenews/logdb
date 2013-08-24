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
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RotationFileLogger extends AbstractLogger {
	private final org.slf4j.Logger slog = org.slf4j.LoggerFactory.getLogger(RotationFileLogger.class);
	private final File dataDir;

	private Pattern datePattern;
	private String charset;
	private SimpleDateFormat dateFormat;
	private Matcher dateMatcher;
	private Matcher beginMatcher;
	private Matcher endMatcher;

	public RotationFileLogger(LoggerSpecification spec, LoggerFactory factory) {
		super(spec, factory);
		this.dataDir = new File(System.getProperty("araqne.data.dir"), "araqne-log-api");
		this.dataDir.mkdirs();

		// optional
		String datePatternRegex = getConfig().get("date_pattern");
		if (datePatternRegex != null)
			datePattern = Pattern.compile(datePatternRegex);

		// optional
		String dateLocale = getConfig().get("date_locale");
		if (dateLocale == null)
			dateLocale = "en";

		// optional
		String dateFormatString = getConfig().get("date_format");
		if (dateFormatString != null)
			dateFormat = new SimpleDateFormat(dateFormatString, new Locale(dateLocale));

		// optional
		String beginRegex = getConfig().get("begin_regex");
		if (beginRegex != null) {
			beginMatcher = Pattern.compile(beginRegex).matcher("");
		}

		String endRegex = getConfig().get("end_regex");
		if (endRegex != null) {
			endMatcher = Pattern.compile(endRegex).matcher("");
		}

		// optional
		charset = getConfig().get("charset");
		if (charset == null)
			charset = "utf-8";
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

			ByteArrayOutputStream logBuf = new ByteArrayOutputStream();

			// last chunk of page which does not contains new line
			ByteArrayOutputStream temp = new ByteArrayOutputStream();
			byte[] b = new byte[128 * 1024];

			while (true) {
				LoggerStatus status = getStatus();
				if (status == LoggerStatus.Stopping || status == LoggerStatus.Stopped)
					break;

				int next = 0;
				int len = is.read(b);
				if (len < 0)
					break;

				for (int i = 0; i < len; i++) {
					if (b[i] == 0xa) {
						buildAndWriteLog(logBuf, b, next, i - next + 1, lastPosition, temp);
						next = i + 1;
					}
				}

				// temp should be matched later (line regex test)
				temp.write(b, next, len - next);
			}

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

	private void buildAndWriteLog(ByteArrayOutputStream logBuf, byte[] b, int offset, int length, AtomicLong lastPosition,
			ByteArrayOutputStream temp) {
		String log = null;
		try {
			log = buildLog(logBuf, b, offset, length, lastPosition, temp);
		} catch (UnsupportedEncodingException e) {
		}

		if (log != null) {
			log = log.trim();
			if (log.length() > 0)
				writeLog(log);
		}
	}

	/**
	 * @param buf
	 *            the buffer which hold partial multiline log
	 * @param b
	 *            read block which contains new line
	 * @param offset
	 *            the new line offset
	 * @param len
	 *            the new line length
	 * @param lastPosition
	 *            the last position which read and written as log
	 * @return new (multiline) log
	 * @throws UnsupportedEncodingException
	 */
	private String buildLog(ByteArrayOutputStream buf, byte[] b, int offset, int len, AtomicLong lastPosition, ByteArrayOutputStream temp)
			throws UnsupportedEncodingException {

		String line = null;
		if (temp.size() > 0) {
			temp.write(b, offset, len);
			line = new String(temp.toByteArray(), charset);
		} else {
			line = new String(b, offset, len, charset);
		}

		if (!line.endsWith("\n")) {
			if (temp.size() == 0)
				temp.write(b, offset, len);

			return null;
		}

		if (beginMatcher != null)
			beginMatcher.reset(line);

		if (endMatcher != null)
			endMatcher.reset(line);

		if (beginMatcher == null && endMatcher == null) {
			if (temp.size() > 0) {
				byte[] t = temp.toByteArray();
				buf.write(t, 0, t.length);
				temp.reset();
			} else {
				buf.write(b, offset, len);
			}

			byte[] old = buf.toByteArray();
			buf.reset();
			lastPosition.addAndGet(old.length);
			return new String(old, charset);
		}

		if (beginMatcher != null && beginMatcher.find()) {
			byte[] old = buf.toByteArray();
			String log = null;

			if (old.length > 0) {
				log = new String(old, charset);
				lastPosition.addAndGet(old.length);
				buf.reset();
			}

			if (temp.size() > 0) {
				byte[] t = temp.toByteArray();
				buf.write(t, 0, t.length);
				temp.reset();
			} else {
				buf.write(b, offset, len);
			}
			return log;
		} else if (endMatcher != null && endMatcher.find()) {
			if (temp.size() > 0) {
				byte[] t = temp.toByteArray();
				buf.write(t, 0, t.length);
				temp.reset();
			} else {
				buf.write(b, offset, len);
			}
			byte[] old = buf.toByteArray();
			lastPosition.addAndGet(old.length);
			String log = new String(old, charset);
			buf.reset();
			return log;
		} else {
			if (temp.size() > 0) {
				byte[] t = temp.toByteArray();
				buf.write(t, 0, t.length);
				temp.reset();
			} else {
				buf.write(b, offset, len);
			}
		}

		return null;
	}

	private void writeLog(String mline) {
		Date d = parseDate(mline);
		Map<String, Object> log = new HashMap<String, Object>();
		log.put("line", mline);
		write(new SimpleLog(d, getFullName(), log));
	}

	protected Date parseDate(String line) {
		if (datePattern == null || dateFormat == null)
			return new Date();

		if (dateMatcher == null)
			dateMatcher = datePattern.matcher(line);
		else
			dateMatcher.reset(line);

		if (!dateMatcher.find())
			return new Date();

		String s = null;
		int count = dateMatcher.groupCount();
		for (int i = 1; i <= count; i++) {
			if (s == null)
				s = dateMatcher.group(i);
			else
				s += dateMatcher.group(i);
		}

		Date d = dateFormat.parse(s, new ParsePosition(0));
		return d != null ? d : new Date();
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
