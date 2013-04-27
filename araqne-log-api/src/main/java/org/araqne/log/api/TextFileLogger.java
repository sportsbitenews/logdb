/*
 * Copyright 2010 NCHOVY
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class TextFileLogger extends AbstractLogger {
	private final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TextFileLogger.class.getName());
	private RotatingLogFileReader reader;
	private DateParser dateParser;

	public TextFileLogger(LoggerSpecification spec, LoggerFactory loggerFactory) throws FileNotFoundException, IOException {
		super("local", spec.getName(), spec.getDescription(), loggerFactory, spec.getLogCount(), spec.getLastLogDate(), spec
				.getConfig());

		Map<String, String> config = spec.getConfig();
		String filePath = config.get("file_path");
		String datePattern = config.get("date_pattern");
		if (datePattern == null)
			datePattern = "MMM dd HH:mm:ss";

		String dateExtractor = config.get("date_extractor");
		if (dateExtractor == null || dateExtractor.isEmpty())
			dateExtractor = DefaultDateParser.dateFormatToRegex(datePattern);

		String dateLocale = config.get("date_locale");
		if (dateLocale == null)
			dateLocale = "en";

		String charset = config.get("charset");
		if (charset == null)
			charset = "utf8";

		this.reader = new RotatingLogFileReader(filePath, Charset.forName(charset));

		String offset = config.get("last_offset");
		String firstLine = config.get("first_line");

		logger.trace("araqne log api: text logger [{}] last offset [{}], last line [{}]", new Object[] { spec.getName(), offset,
				firstLine });

		reader.setFirstLine(firstLine);
		reader.setLastOffset(offset == null ? 0 : Long.valueOf(offset));

		this.dateParser = new DefaultDateParser(new SimpleDateFormat(datePattern, new Locale(dateLocale)), dateExtractor);
	}

	@Override
	protected void runOnce() {
		try {
			this.reader.open();
			while (true) {
				if (getStatus() == LoggerStatus.Stopping || getStatus() == LoggerStatus.Stopped)
					break;

				String line = reader.readLine();
				if (line == null)
					break;

				if (line.isEmpty())
					continue;

				if (logger.isDebugEnabled())
					logger.debug("araqne log api: text logger [{}], read line [{}]", getFullName(), line);

				Date date = dateParser.parse(line);
				if (date == null)
					logger.trace("araqne log api: cannot parse date [{}]", line);

				Map<String, Object> params = new HashMap<String, Object>();
				if (date != null)
					params.put("date", date);
				params.put("line", line);

				Log log = new SimpleLog(date, getFullName(), params);
				write(log);
			}

			getConfig().put("first_line", reader.getFirstLine());
			getConfig().put("last_offset", Long.toString(reader.getLastOffset()));

			logger.trace("araqne log api: name [{}], updated offset [{}]", getName(), reader.getLastOffset());
		} catch (Exception e) {
			logger.error("araqne log api: cannot read log file", e);
		} finally {
			this.reader.close();
		}
	}
}
